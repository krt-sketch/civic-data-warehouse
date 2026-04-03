# from airflow.plugins_manager import AirflowPlugin
# from airflow.providers.common.sql.hooks.sql import DbApiHook


# This is an interesting Claude Code creation. I was looking to remove only a single string from
#  DAG logging. Standard methods of changing log levels on a class do not work for what I wanted.
#  After a two-hour back and forth, Claude came up with the code below. 
# As you can see, it's completely hijacking the DbApiHook run method, creating a logging
#  wrapper, and then sending this new wrapper in place into DbApiHook.
# This is... well, it's great for local debugging. I've certainly done my share of similar
#  things when using .NET. I just don't think it should go outside of my own fork, though.


# class _RowsAffectedSuppressor:
#     """Wraps a logger (stdlib or structlog) and drops 'Rows affected:' info messages."""

#     def __init__(self, original_log):
#         self._orig = original_log

#     def info(self, msg, *args, **kwargs):
#         if "Rows affected:" not in str(msg):
#             self._orig.info(msg, *args, **kwargs)

#     def __getattr__(self, name):
#         return getattr(self._orig, name)


# _original_run = DbApiHook.run


# def _patched_run(self, *args, **kwargs):
#     log = self.log  # force initialisation; works for both stdlib and structlog
#     self._log = _RowsAffectedSuppressor(log)
#     try:
#         return _original_run(self, *args, **kwargs)
#     finally:
#         self._log = log


# DbApiHook.run = _patched_run  # type: ignore[method-assign]


# class SuppressRowsAffectedPlugin(AirflowPlugin):
#     name = "suppress_rows_affected"
