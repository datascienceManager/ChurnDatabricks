I've rebuilt the notebook around one function, `generate_churn_report(period_start, period_end, grace_days=90)`. You can call it for any date range.

```python
generate_churn_report("2026-10-01", "2026-10-31")                 # a month
generate_churn_report("2026-07-01", "2026-09-30")                 # a quarter, or any range
generate_churn_report("2026-10-01", "2026-10-31", grace_days=60)  # another grace period
```

- **Tomorrow's run:** Change `period_start` and `period_end` in the widgets and run the last cell, or call the function from any cell. The deck, charts and a file of every number used go to the output folder.
- **Several months in a row:** Load the subscriber and session data once with `load_dim()` and `load_sessions(end_date)`. Then pass them to the function with `dim=` and `sess=` in a loop. A usage cell in the notebook shows this.
- **Titles and wording:** They follow the dates and the data, so nothing needs editing between periods. A full calendar month gets a month name like "October 2026". Any other range reads as "1 Jul 2026 to 30 Sep 2026". The slides no longer assume that most churners went silent or that one country dominates. They say so only when the numbers show it.
- **Error handling:** It stops with a clear message if the grace-period columns don't exist, if the range has no churned customers, or if nobody tried and failed.
- **One-time setup:** You set the data paths, Synapse host and secret scope once in the configuration cell, not on every run. The scope name is still a placeholder (`CHANGE_ME_SCOPE`), so replace it before the first real run.

I re-ran the sample mode, and the function ran twice in one session. I also checked a quarter range with 60-day grace on the sample figures, which gave a validated deck with correct labels. As before, the Spark queries haven't run against your data, so the first real run is the real test.

Files are in /tmp/build:
- TOD_Churn_Report_Builder.py
