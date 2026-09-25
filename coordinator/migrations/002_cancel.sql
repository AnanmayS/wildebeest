-- Jobs can be cancelled: their PENDING tasks become CANCELLED and are never dispatched or claimed.
alter table tasks drop constraint tasks_state_check;
alter table tasks add constraint tasks_state_check check (state in ('PENDING', 'LEASED', 'SUCCEEDED', 'FAILED', 'CANCELLED'));
