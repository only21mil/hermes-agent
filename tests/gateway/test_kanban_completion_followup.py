import asyncio

from gateway.config import GatewayConfig, HomeChannel, Platform, PlatformConfig
from gateway.run import GatewayRunner
from hermes_cli import kanban_db as kb


class RecordingAdapter:
    def __init__(self):
        self.events = []

    async def handle_message(self, event):
        self.events.append(event)
        return True


class RejectingAdapter:
    async def handle_message(self, event):
        return False


class NoneReturningAdapter:
    async def handle_message(self, event):
        return None


async def _run_one_followup_tick(monkeypatch, runner):
    real_sleep = asyncio.sleep

    async def fake_sleep(delay):
        if delay == 5:
            return None
        runner._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await runner._kanban_completion_followup_watcher(interval=1)


def _make_runner(adapter):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {Platform.TELEGRAM: adapter}
    home = HomeChannel(
        platform=Platform.TELEGRAM,
        chat_id="home-chat",
        name="Home",
        thread_id="topic-1",
    )
    runner.config = GatewayConfig(
        platforms={
            Platform.TELEGRAM: PlatformConfig(
                enabled=True,
                home_channel=home,
            )
        }
    )
    return runner


def _block_task(reason="review-required: inspect this returned handoff"):
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="impl blocked", assignee="knots")
        kb.claim_task(conn, tid)
        task = kb.get_task(conn, tid)
        assert task is not None
        assert kb.block_task(conn, tid, reason=reason, expected_run_id=task.current_run_id)
        event = [ev for ev in kb.list_events(conn, tid) if ev.kind == "blocked"][-1]
        return tid, event.id
    finally:
        conn.close()


def _complete_task(summary="handoff ready"):
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="impl done", assignee="knots")
        assert kb.complete_task(conn, tid, summary=summary)
        event = [ev for ev in kb.list_events(conn, tid) if ev.kind == "completed"][-1]
        return tid, event.id
    finally:
        conn.close()


def _completed_event_id(conn, task_id):
    events = [ev for ev in kb.list_events(conn, task_id) if ev.kind == "completed"]
    assert events
    return events[-1].id


def _make_review_required_flow(conn, reviewer_summary="PASS: source is safe to accept"):
    source = kb.create_task(conn, title="review-required source", assignee="knots")
    kb.claim_task(conn, source)
    source_task = kb.get_task(conn, source)
    assert source_task is not None
    assert kb.block_task(
        conn,
        source,
        reason="review-required: completion follow-up regression coverage",
        expected_run_id=source_task.current_run_id,
    )
    block_event_id = [ev for ev in kb.list_events(conn, source) if ev.kind == "blocked"][-1].id
    reviewer = kb.create_task(
        conn,
        title="review review-required source",
        body="Review-required source: verify the implementation handoff.",
        assignee="node",
        created_by="review-required-router",
    )
    with kb.write_txn(conn):
        kb._append_event(
            conn,
            source,
            "review_required_routed",
            {"reviewer_task_id": reviewer, "handoff_event_id": block_event_id},
        )
    assert kb.complete_task(conn, reviewer, summary=reviewer_summary)
    return source, block_event_id, reviewer, _completed_event_id(conn, reviewer)


def _claim_due_followups(conn, **kwargs):
    return kb.claim_due_completion_followups(
        conn,
        exclude_assignees=["sats"],
        limit=10,
        claimed_by="sats",
        **kwargs,
    )


def _followup_row(event_id):
    conn = kb.connect()
    try:
        return kb.get_completion_followup(conn, event_id)
    finally:
        conn.close()


def test_completion_followup_claims_terminal_blocked_event(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "blocked.db"))
    kb.init_db()
    tid, event_id = _block_task()

    conn = kb.connect()
    try:
        due = kb.claim_due_completion_followups(
            conn,
            exclude_assignees=["sats"],
            limit=10,
            claimed_by="sats",
        )
    finally:
        conn.close()

    assert [item["task_id"] for item in due] == [tid]
    assert [item["event_id"] for item in due] == [event_id]
    assert due[0]["event_kind"] == "blocked"
    row = _followup_row(event_id)
    assert row is not None
    assert row["status"] == "claimed"
    assert row["event_kind"] == "blocked"


def test_completion_followup_enabled_wakes_internal_turn_without_notify_subscription(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "enabled.db"))
    kb.init_db()
    tid, event_id = _complete_task(summary="ready for inspection")
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda *a, **k: {
            "kanban": {
                "completion_followup": {
                    "enabled": True,
                    "target_profile": "sats",
                    "platform": "telegram",
                    "exclude_assignees": ["sats"],
                    "backfill_existing": True,
                }
            }
        },
    )
    monkeypatch.setattr(GatewayRunner, "_active_profile_name", lambda self: "sats")

    adapter = RecordingAdapter()
    asyncio.run(_run_one_followup_tick(monkeypatch, _make_runner(adapter)))
    asyncio.run(_run_one_followup_tick(monkeypatch, _make_runner(adapter)))

    assert len(adapter.events) == 1
    event = adapter.events[0]
    assert event.internal is True
    assert event.source.chat_id == "home-chat"
    assert event.source.thread_id == "topic-1"
    assert tid in event.text
    assert "Event: completed" in event.text
    assert "ready for inspection" in event.text
    row = _followup_row(event_id)
    assert row is not None
    assert row["status"] == "completed"


def test_completion_followup_startup_does_not_backfill_existing_events_but_claims_new(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "no-backfill.db"))
    kb.init_db()
    old_tid, old_event_id = _complete_task(summary="old handoff before watcher start")
    new_event_id_holder = {}
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda *a, **k: {
            "kanban": {
                "completion_followup": {
                    "enabled": True,
                    "target_profile": "sats",
                    "platform": "telegram",
                    "exclude_assignees": ["sats"],
                }
            }
        },
    )
    monkeypatch.setattr(GatewayRunner, "_active_profile_name", lambda self: "sats")

    adapter = RecordingAdapter()
    runner = _make_runner(adapter)
    real_sleep = asyncio.sleep
    interval_sleeps = 0

    async def fake_sleep(delay):
        nonlocal interval_sleeps
        if delay == 5:
            return None
        interval_sleeps += 1
        if interval_sleeps == 1:
            _new_tid, new_event_id_holder["event_id"] = _complete_task(
                summary="new handoff after watcher start"
            )
        else:
            runner._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    asyncio.run(runner._kanban_completion_followup_watcher(interval=1))

    assert len(adapter.events) == 1
    assert old_tid not in adapter.events[0].text
    assert "new handoff after watcher start" in adapter.events[0].text
    assert _followup_row(old_event_id) is None
    new_row = _followup_row(new_event_id_holder["event_id"])
    assert new_row is not None
    assert new_row["status"] == "completed"


def test_completion_followup_claims_event_created_during_startup_delay(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "startup-delay.db"))
    kb.init_db()
    old_tid, old_event_id = _complete_task(summary="old handoff before watcher start")
    new_event_id_holder = {}
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda *a, **k: {
            "kanban": {
                "completion_followup": {
                    "enabled": True,
                    "target_profile": "sats",
                    "platform": "telegram",
                    "exclude_assignees": ["sats"],
                }
            }
        },
    )
    monkeypatch.setattr(GatewayRunner, "_active_profile_name", lambda self: "sats")

    adapter = RecordingAdapter()
    runner = _make_runner(adapter)
    real_sleep = asyncio.sleep

    async def fake_sleep(delay):
        if delay == 5:
            _new_tid, new_event_id_holder["event_id"] = _complete_task(
                summary="new handoff during startup delay"
            )
            return None
        runner._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    asyncio.run(runner._kanban_completion_followup_watcher(interval=1))

    assert len(adapter.events) == 1
    assert old_tid not in adapter.events[0].text
    assert "new handoff during startup delay" in adapter.events[0].text
    assert _followup_row(old_event_id) is None
    new_row = _followup_row(new_event_id_holder["event_id"])
    assert new_row is not None
    assert new_row["status"] == "completed"


def test_completion_followup_adapter_rejection_marks_failed_not_completed(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "rejected.db"))
    kb.init_db()
    _tid, event_id = _complete_task(summary="should fail ledger when adapter rejects")
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda *a, **k: {
            "kanban": {
                "completion_followup": {
                    "enabled": True,
                    "target_profile": "sats",
                    "platform": "telegram",
                    "exclude_assignees": ["sats"],
                    "backfill_existing": True,
                }
            }
        },
    )
    monkeypatch.setattr(GatewayRunner, "_active_profile_name", lambda self: "sats")

    asyncio.run(_run_one_followup_tick(monkeypatch, _make_runner(RejectingAdapter())))

    row = _followup_row(event_id)
    assert row is not None
    assert row["status"] == "failed"
    assert row["completed_at"] is None
    assert row["failed_at"] is not None
    assert row["error"] == "adapter rejected internal completion follow-up"


def test_completion_followup_none_adapter_return_marks_completed(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "none-return.db"))
    kb.init_db()
    _tid, event_id = _complete_task(summary="none return should still mean accepted")
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda *a, **k: {
            "kanban": {
                "completion_followup": {
                    "enabled": True,
                    "target_profile": "sats",
                    "platform": "telegram",
                    "exclude_assignees": ["sats"],
                    "backfill_existing": True,
                }
            }
        },
    )
    monkeypatch.setattr(GatewayRunner, "_active_profile_name", lambda self: "sats")

    asyncio.run(_run_one_followup_tick(monkeypatch, _make_runner(NoneReturningAdapter())))

    row = _followup_row(event_id)
    assert row is not None
    assert row["status"] == "completed"
    assert row["completed_at"] is not None
    assert row["failed_at"] is None
    assert row["error"] is None


def test_completion_followup_failed_row_retries_after_backoff(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "retry.db"))
    kb.init_db()
    _tid, event_id = _complete_task(summary="retry me")

    conn = kb.connect()
    try:
        due = kb.claim_due_completion_followups(conn, exclude_assignees=["sats"], limit=10, claimed_by="sats")
        assert [item["event_id"] for item in due] == [event_id]
        assert kb.mark_completion_followup(conn, event_id, status="failed", error="transient", retry_base_seconds=1)
        row = kb.get_completion_followup(conn, event_id)
        assert row is not None
        assert row["status"] == "failed"
        assert row["retry_count"] == 1
        assert row["next_attempt_at"] is not None
        assert kb.claim_due_completion_followups(conn, exclude_assignees=["sats"], limit=10, claimed_by="sats") == []
        conn.execute(
            "UPDATE kanban_completion_followups SET next_attempt_at = 0 WHERE event_id = ?",
            (event_id,),
        )
        due = kb.claim_due_completion_followups(conn, exclude_assignees=["sats"], limit=10, claimed_by="sats")
        assert [item["event_id"] for item in due] == [event_id]
        row = kb.get_completion_followup(conn, event_id)
        assert row is not None
        assert row["status"] == "claimed"
        assert row["retry_count"] == 1
    finally:
        conn.close()


def test_completion_followup_default_live_only_skips_existing_backlog(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "live-only.db"))
    kb.init_db()
    _tid, event_id = _complete_task(summary="old handoff before watcher start")
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda *a, **k: {
            "kanban": {
                "completion_followup": {
                    "enabled": True,
                    "target_profile": "sats",
                    "platform": "telegram",
                    "exclude_assignees": ["sats"],
                }
            }
        },
    )
    monkeypatch.setattr(GatewayRunner, "_active_profile_name", lambda self: "sats")

    adapter = RecordingAdapter()
    asyncio.run(_run_one_followup_tick(monkeypatch, _make_runner(adapter)))

    assert adapter.events == []
    assert _followup_row(event_id) is None


def test_completion_followup_rejected_internal_turn_is_failed_not_completed(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "rejected.db"))
    kb.init_db()
    _tid, event_id = _complete_task(summary="will be rejected")
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda *a, **k: {
            "kanban": {
                "completion_followup": {
                    "enabled": True,
                    "target_profile": "sats",
                    "platform": "telegram",
                    "exclude_assignees": ["sats"],
                    "backfill_existing": True,
                }
            }
        },
    )
    monkeypatch.setattr(GatewayRunner, "_active_profile_name", lambda self: "sats")

    asyncio.run(_run_one_followup_tick(monkeypatch, _make_runner(RejectingAdapter())))

    row = _followup_row(event_id)
    assert row is not None
    assert row["status"] == "failed"
    assert "rejected internal completion follow-up" in row["error"]


def test_reviewer_pass_and_source_block_terminal_events_still_claim(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "reviewer-pass.db"))
    kb.init_db()

    with kb.connect() as conn:
        _source, block_event_id, reviewer, reviewer_event_id = _make_review_required_flow(conn)
        due = _claim_due_followups(conn)

        assert [item["event_id"] for item in due] == [block_event_id, reviewer_event_id]
        assert [item["event_kind"] for item in due] == ["blocked", "completed"]
        assert [item["task_id"] for item in due][-1] == reviewer
        reviewer_row = kb.get_completion_followup(conn, reviewer_event_id)
        assert reviewer_row is not None
        assert reviewer_row["status"] == "claimed"
        assert reviewer_row["error"] is None


def test_source_acceptance_after_handled_reviewer_pass_is_skipped(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "source-acceptance.db"))
    kb.init_db()

    with kb.connect() as conn:
        source, _block_event_id, _reviewer, reviewer_event_id = _make_review_required_flow(conn)
        due = _claim_due_followups(conn)
        assert reviewer_event_id in [item["event_id"] for item in due]
        assert kb.mark_completion_followup(conn, reviewer_event_id, status="completed")

        assert kb.complete_task(
            conn,
            source,
            summary="Accepted after independent Node PASS from reviewer card.",
        )
        source_event_id = _completed_event_id(conn, source)

        assert _claim_due_followups(conn) == []
        row = kb.get_completion_followup(conn, source_event_id)
        assert row is not None
        assert row["status"] == "skipped"
        assert row["event_kind"] == "completed"
        assert row["error"] == "review_required_source_acceptance_after_reviewer_pass"


def test_source_acceptance_after_unhandled_reviewer_pass_still_claims(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "unhandled-reviewer-pass.db"))
    kb.init_db()

    with kb.connect() as conn:
        source, _block_event_id, _reviewer, _reviewer_event_id = _make_review_required_flow(conn)
        assert kb.complete_task(conn, source, summary="Accepted before PASS follow-up was handled.")
        source_event_id = _completed_event_id(conn, source)

        due = _claim_due_followups(conn)
        assert source_event_id in [item["event_id"] for item in due]
        row = kb.get_completion_followup(conn, source_event_id)
        assert row is not None
        assert row["status"] == "claimed"
        assert row["error"] is None


def test_source_acceptance_after_reviewer_fail_still_claims(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "reviewer-fail.db"))
    kb.init_db()

    with kb.connect() as conn:
        source, _block_event_id, _reviewer, reviewer_event_id = _make_review_required_flow(
            conn,
            reviewer_summary="FAIL: changes requested before source acceptance",
        )
        due = _claim_due_followups(conn)
        assert reviewer_event_id in [item["event_id"] for item in due]
        assert kb.mark_completion_followup(conn, reviewer_event_id, status="completed")

        assert kb.complete_task(conn, source, summary="Accepted despite reviewer rejection text")
        source_event_id = _completed_event_id(conn, source)

        due = _claim_due_followups(conn)
        assert source_event_id in [item["event_id"] for item in due]
        row = kb.get_completion_followup(conn, source_event_id)
        assert row is not None
        assert row["status"] == "claimed"
        assert row["error"] is None


def test_unrelated_source_completion_is_not_skipped_by_prior_reviewer_pass(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "unrelated-source.db"))
    kb.init_db()

    with kb.connect() as conn:
        _source_a, _block_event_id, _reviewer_a, reviewer_event_id = _make_review_required_flow(conn)
        due = _claim_due_followups(conn)
        assert reviewer_event_id in [item["event_id"] for item in due]
        assert kb.mark_completion_followup(conn, reviewer_event_id, status="completed")

        source_b = kb.create_task(conn, title="separate review-required source", assignee="knots")
        kb.claim_task(conn, source_b)
        source_b_task = kb.get_task(conn, source_b)
        assert source_b_task is not None
        assert kb.block_task(
            conn,
            source_b,
            reason="review-required: independent source without handled reviewer PASS",
            expected_run_id=source_b_task.current_run_id,
        )
        assert kb.complete_task(conn, source_b, summary="Accepted by manual inspection")
        source_b_event_id = _completed_event_id(conn, source_b)

        due = _claim_due_followups(conn)
        assert [item["task_id"] for item in due] == [source_b, source_b]
        assert [item["event_kind"] for item in due] == ["blocked", "completed"]
        assert [item["event_id"] for item in due][-1] == source_b_event_id
        row = kb.get_completion_followup(conn, source_b_event_id)
        assert row is not None
        assert row["status"] == "claimed"
        assert row["error"] is None


def test_completion_followup_excluded_assignee_prevents_loop(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "excluded.db"))
    kb.init_db()
    _, event_id = _complete_task(summary="self completion")
    conn = kb.connect()
    try:
        task_id = kb.get_completion_followup(conn, event_id)
        assert task_id is None
        rows = kb.claim_due_completion_followups(
            conn,
            exclude_assignees=["knots"],
            limit=10,
            claimed_by="sats",
        )
    finally:
        conn.close()

    assert rows == []
    row = _followup_row(event_id)
    assert row is not None
    assert row["status"] == "skipped"
    assert row["error"] == "excluded_assignee"
