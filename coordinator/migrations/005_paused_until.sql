-- End of the pause the coordinator injected (POST /workers/:id/pause). A lease lost by a task that
-- started before this moment is attributed to the pause and costs no attempt; see requeueLostLeases.
alter table workers add column paused_until timestamptz null;
