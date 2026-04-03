# from airflow.plugins_manager import AirflowPlugin
# from airflow.providers.common.sql.hooks.sql import DbApiHook


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
