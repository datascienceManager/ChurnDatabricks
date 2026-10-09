# Databricks notebook source
# MAGIC %md
# MAGIC # TOD Churn Intelligence Report: notebook that builds the deck
# MAGIC
# MAGIC Runs the churn analysis for one churn period (default: **September 2026, 90-day grace period**) and writes a 16-slide PowerPoint in the TOD template.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Builds the churn cohort from the subscriber dimension (`Subscription_type_gp_<grace> = 'churned'` in the period; non-commercial, daily/weekly and promo subscriptions excluded)
# MAGIC 2. Matches every churned customer to viewer sessions and buckets viewing minutes two ways: in the subscription that churned, and in any subscription
# MAGIC 3. Deep-dives the customers with no viewing: raw sessions, last login, country, payment method, offer, subscription age
# MAGIC 4. For the customers who tried and failed, classifies the errors and retry behaviour
# MAGIC 5. Draws the charts and builds the deck. Slide titles and key points are written from the computed numbers
# MAGIC
# MAGIC **Before the first run**
# MAGIC * Set the widgets at the top of the *Parameters* cell: period, subscriber-dimension path, Synapse host/database and the secret scope that holds the JDBC user and password
# MAGIC * Set `use_sample_metrics = true` to build the deck from the September 2026 figures embedded in this notebook, with no database access. Use it to check the template and output folder
# MAGIC
# MAGIC **Output**: `TOD_Churn_<period>_Intelligence_Report.pptx`, the charts and `metrics.json` (every number used in the deck) in the output folder

# COMMAND ----------

# MAGIC %pip install python-pptx==1.0.2 -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# ---------------------------------------------------------------
# Parameters (widgets) and small helpers
# ---------------------------------------------------------------
import os, io, json, math, base64, datetime as dt, calendar, warnings
warnings.filterwarnings("ignore")

def W(name, default, label=None):
    """Read a notebook widget; fall back to the default when run outside Databricks."""
    try:
        dbutils.widgets.text(name, default, label or name)
        return dbutils.widgets.get(name).strip()
    except NameError:
        return default

P = dict(
    period_start   = W("period_start", "2026-09-01", "Churn period start (YYYY-MM-DD)"),
    period_end     = W("period_end",   "2026-09-30", "Churn period end (YYYY-MM-DD)"),
    grace_days     = int(W("grace_days", "90", "Grace period (days): selects Subscription_type_gp_<n>")),
    viewing_start  = W("viewing_start", "2022-01-01", "Earliest viewing date to load"),
    dim_path       = W("dim_path", "dbfs:/mnt/dimsubscriber/parquet/ALL/DIMSubscriber", "Subscriber dimension (parquet)"),
    jdbc_host      = W("jdbc_host", "beflixdwh-01.sql.azuresynapse.net", "Synapse host"),
    jdbc_db        = W("jdbc_db", "DWH", "Synapse database"),
    secret_scope   = W("secret_scope", "CHANGE_ME_SCOPE", "Secret scope with the JDBC login"),
    secret_user    = W("secret_user_key", "jdbc-user", "Secret key: JDBC user"),
    secret_pass    = W("secret_pass_key", "jdbc-password", "Secret key: JDBC password"),
    output_dir     = W("output_dir", "/dbfs/FileStore/tod_churn_reports", "Output folder"),
    use_sample     = W("use_sample_metrics", "false", "true = build from embedded Sep 2026 figures").lower() == "true",
)

# Payment methods grouped as "app store, web, card, wallet" on the payment slide.
# Everything else is treated as telco / partner billing. Edit if new methods appear.
APP_PAYMENTS = ["iOS", "web", "Card", "Android", "Apple Pay", "PayPal", "Google Pay", "storekit2-sandbox"]
# Optional country hint shown next to partner billing names on the slides.
PARTNER_HINT = {"1001TV_IQ": "Iraq", "Richatt_MR": "Mauritania", "Zain_KW": "Kuwait"}

# Countries loaded from the viewer-session view (same list as the original notebook)
VIEW_COUNTRIES = ["algeria", "bahrain", "egypt", "iraq", "jordan", "kuwait", "lebanon", "libya", "morocco", "oman",
                  "qatar", "saudi arabia", "syria", "tunisia", "united arab emirates", "yemen",
                  "occupied palestinian territory", "south sudan", "mauritania", "sudan", "somalia", "chad"]

D_START = dt.date.fromisoformat(P["period_start"]); D_END = dt.date.fromisoformat(P["period_end"])

def period_labels(s, e):
    full_month = s.day == 1 and e.day == calendar.monthrange(e.year, e.month)[1] and (s.year, s.month) == (e.year, e.month)
    if full_month:
        return dict(long=f"{e:%B %Y}", short=f"{e:%b %Y}", month=f"{e:%B}", file=f"{e:%b%Y}",
                    between=f"between 1 and {e.day} {e:%B %Y}", upper=f"{e:%B %Y}".upper(), inn=f"in {e:%B}")
    rng = f"{s.day} {s:%b %Y} to {e.day} {e:%b %Y}"
    return dict(long=rng, short=rng, month=rng, file=f"{s:%b%Y}-{e:%b%Y}", between=f"from {rng}", upper=rng.upper(), inn=f"from {rng}")

LBL = period_labels(D_START, D_END)
LBL["cut"] = f"{D_END.day} {D_END:%b}"
LBL["grace"] = f"{P['grace_days']}-day"

K = lambda v: f"{int(round(v)):,}"
def pc(a, b, d=1): return f"{(a / b * 100 if b else 0):.{d}f}%"
os.makedirs(P["output_dir"], exist_ok=True)
print("Period:", LBL["long"], "| grace:", P["grace_days"], "days | sample mode:", P["use_sample"])

# COMMAND ----------

# ---------------------------------------------------------------
# Load data (skipped in sample mode)
#   dim  : subscriber dimension, one row per subscription
#   sess : viewer sessions (raw), with only the columns this analysis needs
# ---------------------------------------------------------------
from pyspark.sql import functions as F, Window

dim = sess = None
if not P["use_sample"]:
    dim = spark.read.parquet(P["dim_path"])

    user = globals().get("jdbcUsername") or dbutils.secrets.get(P["secret_scope"], P["secret_user"])
    pwd  = globals().get("jdbcPassword") or dbutils.secrets.get(P["secret_scope"], P["secret_pass"])
    jdbc_url = f"jdbc:sqlserver://{P['jdbc_host']}:1433;database={P['jdbc_db']}"
    countries_sql = ", ".join(f"'{c}'" for c in VIEW_COUNTRIES)

    # Same filters as the original notebook: MENA countries, valid viewer id, valid asset id.
    sess_query = f"""
        select viewerid, startdate, asset_id, playingunixtimems, country,
               errorlist, startuperror, deviceos, cdn
        from [dbviews].[ViewerSessions] (nolock)
        where country in ({countries_sql})
          and startdate >= '{P['viewing_start']}' and startdate <= '{P['period_end']}'
          and viewerid not like '%.%'
          and (asset_id LIKE '%-%' OR asset_id NOT LIKE '%[^0-9]%')
    """
    sess = (spark.read.format("jdbc").option("url", jdbc_url).option("user", user).option("password", pwd)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("query", sess_query).load()
            .withColumnRenamed("viewerid", "viewerId"))
    print("dim columns:", len(dim.columns), "| session columns:", sess.columns)

# COMMAND ----------

# ---------------------------------------------------------------
# compute_metrics: every number in the deck is produced here
# ---------------------------------------------------------------
BUCKETS = ["No viewing", "0-1 min", "1-10 min", ">10 min"]
LOGIN_ORDER = ["Never logged in", "Within 7 days", "8-30 days", "31-90 days", "91-180 days", "180+ days"]
DUR_BUCKETS = [("0-7 days", 0, 7), ("8-30 days", 8, 30), ("31-90 days", 31, 90),
               ("91-180 days", 91, 180), ("181-365 days", 181, 365), ("365+ days", 366, 10**6)]
RETRY_ORDER = ["1 attempt", "2-3", "4-10", "11-50", "50+"]
CAUSE_PRIORITY = {"entitlement": 1, "drm": 2, "cdn": 3, "ios": 4, "silent": 5, "other": 6}

def compute_metrics(dim, sess, P):
    start, end, g = P["period_start"], P["period_end"], P["grace_days"]
    status_col, date_col = f"Subscription_type_gp_{g}", f"Subscription_calender_date_gp_{g}"
    end_d = F.lit(end).cast("date")
    M = dict(period_start=start, period_end=end, grace_days=g)

    # ---- 1. Churn cohort -------------------------------------------------
    cid = F.trim(F.col("Customer_External_ID"))
    rows = (dim.filter((F.col("non_commercial") != 1) & (F.col("daily_weekly_flag") != 1) & (F.col("promo") != 1)
                       & (F.col(status_col) == "churned")
                       & (F.col(date_col) >= start) & (F.col(date_col) <= end)
                       & F.col("Customer_External_ID").isNotNull() & (cid != "") & (cid != "null"))
            .withColumn("cid", F.col("Customer_External_ID"))
            .withColumn("sub_start", F.col("Subscription_start_date").cast("date")))
    rows = rows.cache()
    M["total"] = rows.select("cid").distinct().count()
    M["subs"] = rows.count()                                                     # churned subscription rows
    M["subs_distinct"] = rows.select("cid", "Subscription_ID", "sub_start").distinct().count()
    subs_by_cust = rows.select("cid", "sub_start").distinct().groupBy("cid").count()
    M["multi_start_customers"] = subs_by_cust.filter(F.col("count") > 1).count()
    print(f"Cohort: {M['total']:,} customers | {M['subs']:,} subscription rows | {M['subs_distinct']:,} distinct (customer, subscription, start)")

    # ---- 2. Viewing minutes per customer, in / outside the churned subscription ----
    v = (sess.select(F.col("viewerId"), F.col("startdate").cast("date").alias("startdate"), "asset_id",
                     (F.col("playingunixtimems").cast("double") / 60000).alias("mins"))
         .filter(F.col("startdate") <= end_d)
         .filter(~(F.col("asset_id").isNull() | (F.col("asset_id") == "") | (F.col("asset_id") == "null")))
         .filter(~(F.col("viewerId").isNull() | (F.col("viewerId") == "") | (F.col("viewerId") == "null"))))
    all_subs = rows.select("cid", "sub_start").distinct()
    j = all_subs.join(v, all_subs.cid == v.viewerId, "left")
    j = j.withColumn("in_win", F.when(F.col("startdate").isNull(), F.lit(None).cast("int"))
                     .otherwise(((F.col("startdate") >= F.col("sub_start")) & (F.col("startdate") <= end_d)).cast("int")))
    cust = (j.groupBy("cid").agg(
                F.coalesce(F.sum(F.when(F.col("in_win") == 1, F.col("mins"))), F.lit(0.0)).alias("win_mins"),
                F.coalesce(F.sum(F.when(F.col("in_win") == 0, F.col("mins"))), F.lit(0.0)).alias("out_mins"))
            .withColumn("any_mins", F.col("win_mins") + F.col("out_mins")))
    def bucket(c):
        return (F.when(c <= 0, "No viewing").when(c <= 1, "0-1 min").when(c <= 10, "1-10 min").otherwise(">10 min"))
    cust = (cust.withColumn("win_bucket", bucket(F.col("win_mins"))).withColumn("any_bucket", bucket(F.col("any_mins")))
            .withColumn("wtype", F.when((F.col("win_mins") > 0) & (F.col("out_mins") == 0), "Sub window only")
                        .when((F.col("win_mins") == 0) & (F.col("out_mins") > 0), "Outside window only")
                        .when((F.col("win_mins") == 0) & (F.col("out_mins") == 0), "No viewing")
                        .otherwise("Both"))).cache()
    def counts(col):
        d = {r[0]: r[1] for r in cust.groupBy(col).count().collect()}
        return d
    win, anyt, wt = counts("win_bucket"), counts("any_bucket"), counts("wtype")
    M["win"] = {b: int(win.get(b, 0)) for b in BUCKETS}
    M["anyt"] = {b: int(anyt.get(b, 0)) for b in BUCKETS}
    M["wt"] = {k: int(wt.get(k, 0)) for k in ["Sub window only", "Both", "Outside window only", "No viewing"]}
    M["zero"] = M["anyt"]["No viewing"]

    # ---- 3. The no-viewing group: who tried (raw sessions) and who never opened a session ----
    nv = cust.filter(F.col("any_mins") == 0).select("cid")
    raw = sess.join(F.broadcast(nv), sess.viewerId == nv.cid, "inner").cache()
    zero_play = F.col("playingunixtimems").isNull() | (F.col("playingunixtimems").cast("double") <= 0)
    missing_asset = F.col("asset_id").isNull() | (F.col("asset_id") == "") | (F.col("asset_id") == "null")
    st = raw.agg(F.count("*").alias("sessions"), F.countDistinct("viewerId").alias("viewers"),
                 F.sum(zero_play.cast("int")).alias("zero_play"),
                 F.sum(missing_asset.cast("int")).alias("missing_asset"),
                 F.countDistinct(F.when(missing_asset, F.col("viewerId"))).alias("missing_asset_viewers"),
                 F.sum(F.when(missing_asset, F.col("playingunixtimems").cast("double") / 60000)).alias("missing_asset_mins")).first()
    M["failed"] = int(st["viewers"]); M["nosess"] = M["zero"] - M["failed"]
    M["raw_sessions"] = int(st["sessions"]); M["raw_zero_play"] = int(st["zero_play"])
    M["raw_missing_asset"] = int(st["missing_asset"] or 0); M["raw_missing_asset_viewers"] = int(st["missing_asset_viewers"] or 0)
    M["raw_missing_asset_mins"] = round(float(st["missing_asset_mins"] or 0), 2)
    tried = raw.select(F.col("viewerId").alias("cid")).distinct().withColumn("tried", F.lit(1))

    # attributes of the no-viewing customers (one row per churned subscription, as in the original notebook)
    nvd = (rows.join(F.broadcast(nv), "cid", "inner").join(F.broadcast(tried), "cid", "left")
           .withColumn("tried", F.coalesce(F.col("tried"), F.lit(0))))
    nvd = nvd.cache()
    def cd(col): return F.countDistinct("cid").alias("n")

    # last login recency
    days_login = F.datediff(end_d, F.col("Last_login_date").cast("date"))
    login_lbl = (F.when(F.col("Last_login_date").isNull(), "Never logged in").when(days_login <= 7, "Within 7 days")
                 .when(days_login <= 30, "8-30 days").when(days_login <= 90, "31-90 days")
                 .when(days_login <= 180, "91-180 days").otherwise("180+ days"))
    ld = {r[0]: r[1] for r in nvd.groupBy(login_lbl.alias("b")).agg(cd("cid")).collect()}
    M["login"] = [(k, int(ld[k])) for k in LOGIN_ORDER if ld.get(k)]

    # country
    M["country"] = {(r[0] or "Unknown"): int(r[1]) for r in nvd.groupBy("Country").agg(cd("cid")).orderBy(F.desc("n")).collect()}

    # payment method x tried / no session
    pay = {}
    for r in nvd.groupBy("Payment_method", "tried").agg(cd("cid")).collect():
        a = pay.setdefault(r[0] or "Unknown", [0, 0]); a[1 if r[1] == 1 else 0] = int(r[2])
    M["pay"] = {k: tuple(v) for k, v in pay.items()}
    M["pay_total"] = {(r[0] or "Unknown"): int(r[1]) for r in nvd.groupBy("Payment_method").agg(cd("cid")).collect()}

    # offer type and direct / indirect
    M["offer"] = [(r[0] or "Unknown", r[1] or "Unknown", int(r[2])) for r in
                  nvd.groupBy("Offer_type", "Dir_indir").agg(cd("cid")).orderBy(F.desc("n")).collect()]

    # subscription age at the cut-off
    age = F.datediff(end_d, F.col("sub_start"))
    expr = None
    for lbl, lo, hi in DUR_BUCKETS:
        cond = (age >= lo) & (age <= hi)
        expr = F.when(cond, lbl) if expr is None else expr.when(cond, lbl)
    dd = {r[0]: r[1] for r in nvd.groupBy(expr.otherwise("Other").alias("b")).agg(cd("cid")).collect()}
    M["dur"] = [(lbl, int(dd[lbl]), lo, hi) for lbl, lo, hi in DUR_BUCKETS if dd.get(lbl)]

    # ---- 4. The customers who tried: retries, startup flag, error causes ----
    per_v = raw.groupBy("viewerId").agg(F.count("*").alias("n"), F.sum(zero_play.cast("int")).alias("err"))
    rb = (F.when(F.col("n") == 1, "1 attempt").when(F.col("n") <= 3, "2-3").when(F.col("n") <= 10, "4-10")
          .when(F.col("n") <= 50, "11-50").otherwise("50+"))
    rr = {r[0]: r[1:] for r in per_v.groupBy(rb.alias("b")).agg(F.count("*"), F.sum("n"), F.sum("err")).collect()}
    M["retry"] = [(k, int(rr[k][0]), int(rr[k][1]), int(rr[k][2])) for k in RETRY_ORDER if k in rr]

    failed_s = raw.filter(zero_play)
    su = failed_s.groupBy(F.coalesce(F.col("startuperror").cast("string"), F.lit("NULL")).alias("flag")) \
                 .agg(F.count("*").alias("s"), F.countDistinct("viewerId").alias("c")).collect()
    M["startup"] = {r["flag"]: (int(r["s"]), int(r["c"])) for r in su}

    err = F.upper(F.coalesce(F.col("errorlist"), F.lit("")))
    cause = (F.when(F.trim(err) == "", "silent").when(err.contains("NOT_ENTITLED"), "entitlement")
             .when(err.contains("DRM"), "drm").when(err.contains("NSURLERRORDOMAIN") | err.contains("-1102"), "ios")
             .when(err.contains("DASH MANIFEST") | err.contains("403"), "cdn").otherwise("other"))
    prio = F.create_map(*[x for k, v in CAUSE_PRIORITY.items() for x in (F.lit(k), F.lit(v))])
    fs = failed_s.select(F.col("viewerId").alias("cid"), cause.alias("cause"))
    pv = fs.groupBy("cid", "cause").count().withColumn("p", prio[F.col("cause")])
    w = Window.partitionBy("cid").orderBy(F.desc("count"), F.asc("p"))
    primary = pv.withColumn("rn", F.row_number().over(w)).filter("rn = 1").select("cid", F.col("cause").alias("primary"))
    fcount = fs.groupBy("cid").agg(F.count("*").alias("fsess"))
    ctry = rows.groupBy("cid").agg(F.min("Country").alias("country"))
    per_c = (tried.select("cid").join(primary, "cid", "left").join(fcount, "cid", "left").join(ctry, "cid", "left")
             .withColumn("primary", F.coalesce(F.col("primary"), F.lit("other")))
             .withColumn("fsess", F.coalesce(F.col("fsess"), F.lit(0)))
             .withColumn("country", F.coalesce(F.col("country"), F.lit("Unknown"))))
    cc = per_c.groupBy("primary", "country").agg(F.count("*").alias("c"), F.sum("fsess").alias("s")).collect()
    causes = {}
    for r in cc:
        d = causes.setdefault(r["primary"], dict(customers=0, sessions=0, countries={}))
        d["customers"] += int(r["c"]); d["sessions"] += int(r["s"])
        d["countries"][r["country"]] = (int(r["c"]), int(r["s"]))
    M["causes"] = {k: dict(customers=v["customers"], sessions=v["sessions"],
                           countries=sorted(((c, n, s) for c, (n, s) in v["countries"].items()), key=lambda t: -t[1]))
                   for k, v in causes.items()}

    # ---- 5. Reconciliation checks (printed; they do not stop the run) ----
    chk = []
    chk.append(("window buckets add to total", sum(M["win"].values()) == M["total"]))
    chk.append(("any-viewing buckets add to total", sum(M["anyt"].values()) == M["total"]))
    chk.append(("watching types add to total", sum(M["wt"].values()) == M["total"]))
    chk.append(("no-viewing = earlier-only + none in window", M["win"]["No viewing"] == M["wt"]["Outside window only"] + M["wt"]["No viewing"]))
    chk.append(("login buckets add to no-viewing group", sum(n for _, n in M["login"]) == M["zero"]))
    chk.append(("cause customers add to tried", sum(c["customers"] for c in M["causes"].values()) == M["failed"]))
    chk.append(("retry customers add to tried", sum(r[1] for r in M["retry"]) == M["failed"]))
    M["checks"] = [(n, bool(ok)) for n, ok in chk]
    for n, ok in M["checks"]:
        print(("OK   " if ok else "FAIL ") + n)
    return M

# COMMAND ----------

# ---------------------------------------------------------------
# Embedded September 2026 figures (used only when use_sample_metrics = true)
# ---------------------------------------------------------------
def sample_metrics():
    M = dict(period_start="2026-09-01", period_end="2026-09-30", grace_days=90,
             total=113610, subs=116294, subs_distinct=115146, multi_start_customers=718,
             win={"No viewing": 35587, "0-1 min": 3494, "1-10 min": 3585, ">10 min": 70944},
             anyt={"No viewing": 24651, "0-1 min": 2865, "1-10 min": 2868, ">10 min": 83226},
             wt={"Sub window only": 38411, "Both": 39612, "Outside window only": 10936, "No viewing": 24651},
             zero=24651, nosess=23836, failed=815, raw_sessions=4002, raw_zero_play=3996,
             raw_missing_asset=27, raw_missing_asset_viewers=15, raw_missing_asset_mins=2.94,
             login=[("Within 7 days", 105), ("8-30 days", 224), ("31-90 days", 484), ("91-180 days", 16939), ("180+ days", 6899)],
             country={"Iraq": 10643, "Egypt": 5398, "Mauritania": 3200, "Kuwait": 2283, "United Arab Emirates": 927, "Qatar": 415,
                      "Oman": 339, "Algeria": 319, "Jordan": 314, "Morocco": 273, "Saudi Arabia": 125, "Bahrain": 120, "Others": 91,
                      "Tunisia": 86, "Libya": 72, "Lebanon": 33, "Israel": 5, "Occupied Palestinian Territory": 5, "Sudan": 2, "Yemen": 1},
             offer=[("Subscription", "Indirect", 16209), ("Pass", "Indirect", 5715), ("Subscription", "Direct", 2366), ("Pass", "Direct", 367)],
             dur=[("31-90 days", 154, 31, 90), ("91-180 days", 20400, 91, 180), ("181-365 days", 1851, 181, 365), ("365+ days", 2255, 366, 10**6)],
             retry=[("1 attempt", 340, 340, 338), ("2-3", 209, 488, 488), ("4-10", 181, 1140, 1136), ("11-50", 79, 1627, 1627), ("50+", 6, 407, 407)],
             startup={"1": (3085, 506), "0": (911, 420)})
    pay = {"1001TV_IQ": (10442, 91), "Richatt_MR": (3170, 26), "Vodafone Egypt": (2548, 115), "Zain_KW": (2004, 37), "iOS": (1599, 252),
           "web": (1267, 117), "Orange Egypt": (682, 29), "Etisalat2_EG": (615, 22), "Card": (411, 49), "DJEZZY_DZ": (206, 2),
           "Omantel_OM": (190, 5), "Android": (145, 28), "Apple Pay": (104, 13), "Orange_JO": (99, 5), "Umniah_JO": (70, 2),
           "Ooredoo_TN": (65, 1), "PayPal": (47, 12), "Google Pay": (44, 5), "Maroc_MA": (34, 1), "Ooredoo_DZ": (26, 1), "Zain_BH": (18, 1),
           "Proxym_TN": (13, 0), "Epay_AE": (6, 0), "Tpay_Vodafone_EG_v2": (5, 0), "Zain_JO": (5, 0), "Tpay_DU_AE_v2": (4, 0),
           "Tpay_Omantel_OM": (3, 1), "INWI_MA": (3, 0), "Orange_MA": (3, 0), "Tpay_Orange_EG_v2": (3, 0), "storekit2-sandbox": (2, 0),
           "Tpay_Orange_JO_v2": (2, 0), "Tpay_Orange_MA_v2": (1, 0), "Zain_SD": (1, 0), "Tpay_Vodafone_QA_v2": (1, 0)}
    tot = {"1001TV_IQ": 10533, "Richatt_MR": 3196, "Vodafone Egypt": 2663, "Zain_KW": 2041, "iOS": 1851, "web": 1384, "Orange Egypt": 711,
           "Etisalat2_EG": 637, "Card": 460, "DJEZZY_DZ": 208, "Omantel_OM": 195, "Android": 173, "Apple Pay": 117, "Orange_JO": 104,
           "Umniah_JO": 72, "Ooredoo_TN": 66, "PayPal": 59, "Google Pay": 49, "Maroc_MA": 35, "Ooredoo_DZ": 27, "Zain_BH": 19,
           "Proxym_TN": 13, "Epay_AE": 6, "Tpay_Vodafone_EG_v2": 5, "Zain_JO": 5, "Tpay_Omantel_OM": 4, "Tpay_DU_AE_v2": 4, "INWI_MA": 3,
           "Orange_MA": 3, "Tpay_Orange_EG_v2": 3, "storekit2-sandbox": 2, "Tpay_Orange_JO_v2": 2, "Tpay_Orange_MA_v2": 1, "Zain_SD": 1,
           "Tpay_Vodafone_QA_v2": 1}
    M["pay"], M["pay_total"] = pay, tot
    # Dominant error per country, as in the first analysis: (country, customers, sessions)
    M["causes"] = {
        "silent": dict(customers=265, sessions=533, countries=[("Egypt", 172, 360), ("Iraq", 55, 83), ("Mauritania", 17, 60), ("Bahrain", 12, 18), ("Algeria", 9, 12)]),
        "entitlement": dict(customers=55, sessions=388, countries=[("United Arab Emirates", 29, 140), ("Kuwait", 15, 158), ("Jordan", 7, 50), ("Oman", 4, 40)]),
        "ios": dict(customers=14, sessions=159, countries=[("Qatar", 9, 121), ("Lebanon", 2, 20), ("Syria", 2, 11), ("Tunisia", 1, 7)]),
        "cdn": dict(customers=10, sessions=49, countries=[("Morocco", 8, 47), ("Occupied Palestinian Territory", 1, 1), ("Yemen", 1, 1)]),
        "drm": dict(customers=6, sessions=184, countries=[("Saudi Arabia", 4, 162), ("Libya", 2, 22)]),
        "other": dict(customers=465, sessions=0, countries=[]),
    }
    M["checks"] = [("embedded September 2026 figures (no reconciliation run)", True)]
    return M

# ---------------------------------------------------------------
# Run the analysis (or load the embedded figures) and keep the numbers
# ---------------------------------------------------------------
M = sample_metrics() if P["use_sample"] else compute_metrics(dim, sess, P)
M["label"] = LBL
metrics_path = os.path.join(P["output_dir"], f"metrics_{LBL['file']}.json")
with open(metrics_path, "w") as fh:
    json.dump(M, fh, indent=2, default=str)
print("Saved", metrics_path)

# COMMAND ----------

# ---------------------------------------------------------------
# Derived figures + charts (TOD chart style)
# ---------------------------------------------------------------
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
from types import SimpleNamespace as NS_

CAUSE_INFO = {
    "silent": dict(label="Silent failure", chart="Silent failure (no error code)", title="Silent Failures", plural="silent failures",
                   saw="Playback never starts and no error is logged", check="Player error capture; app crashes and timeouts",
                   root="Unknown; telemetry gap", prio="High", tail="playback never started and no error was logged"),
    "entitlement": dict(label="Entitlement denied", chart="Entitlement denied (not entitled)", title="Entitlement Denials", plural="entitlement denials",
                        saw="\"You do not have access to this content. Please visit tod.tv to upgrade.\"",
                        check="Billing and entitlement sync for paying subscribers", root="Billing and entitlement sync", prio="Critical",
                        tail="shown to subscribers"),
    "ios": dict(label="iOS permission", chart="iOS permission denied (-1102)", title="iOS Permission Errors", plural="iOS permission errors",
                saw="NSURLErrorDomain -1102: no permission to access the resource", check="Current iOS build, network and certificate settings",
                root="App or network setting", prio="Medium", tail=""),
    "cdn": dict(label="CDN / geo block", chart="CDN / geo block (403, DASH manifest)", title="CDN Blocks", plural="CDN blocks",
                saw="\"Failed to load DASH manifest: 403 Forbidden\" (code 1208)", check="Content rights and CDN authentication by region",
                root="Rights or CDN set-up", prio="High", tail=""),
    "drm": dict(label="DRM licence", chart="DRM licence failure", title="DRM Failures", plural="DRM failures",
                saw="DRM licence request failed (2003) or media key initialisation failed (2008)",
                check="DRM licence server and device compatibility", root="DRM set-up or device", prio="High", tail=""),
    "other": dict(label="Tried, other errors", chart="Other errors (not broken out)", title="Other Errors", plural="other errors",
                  saw="", check="Break out the remaining error codes", root="Not yet broken out", prio="To analyse", tail=""),
}
nice_country = lambda c: c if c in ("Others", "Unknown") else str(c).title()

def derive(M):
    d = NS_()
    d.T, d.S, d.Z, d.NS, d.FL = M["total"], M["subs"], M["zero"], M["nosess"], M["failed"]
    d.win, d.anyt, d.wt = M["win"], M["anyt"], M["wt"]
    d.login = M["login"]; d.ld = dict(M["login"])
    ctry = {}
    for k, v in M["country"].items(): ctry[nice_country(k)] = ctry.get(nice_country(k), 0) + v
    d.country = sorted(ctry.items(), key=lambda t: -t[1])
    d.pay, d.pay_total = M["pay"], M["pay_total"]
    d.app = [m for m in d.pay_total if m in APP_PAYMENTS]; d.ptn = [m for m in d.pay_total if m not in APP_PAYMENTS]
    d.app_n = sum(d.pay_total[m] for m in d.app); d.ptn_n = sum(d.pay_total[m] for m in d.ptn)
    d.app_tried = sum(d.pay[m][1] for m in d.app if m in d.pay); d.ptn_tried = sum(d.pay[m][1] for m in d.ptn if m in d.pay)
    d.top_ptn = sorted(d.ptn, key=lambda m: -d.pay_total[m])[:4]
    d.top_ptn_n = sum(d.pay_total[m] for m in d.top_ptn)
    d.retry = M["retry"]
    d.causes = {k: dict(v, **CAUSE_INFO[k]) for k, v in M["causes"].items() if k in CAUSE_INFO}
    d.named = sorted([(k, v) for k, v in d.causes.items() if k != "other" and v["customers"] > 0], key=lambda t: -t[1]["customers"])
    d.other_c = d.causes.get("other", dict(customers=0, sessions=0))["customers"]
    d.offer_total = sum(n for _, _, n in M["offer"]); d.dur_total = sum(n for _, n, _, _ in M["dur"])
    return d
d = derive(M)

# ---------------- chart style ----------------
BG = "#FAFBFD"; INK = "#222B3A"; GRID = "#EEEEEE"; MUTE = "#777777"
TEAL = "#3B5563"; ORG = "#E98068"; YEL = "#FFBD00"; BLU = "#86A2D4"; GRY = "#B8C0C8"; GRN = "#2A9D8F"; RED = "#C0392B"
DPI = 220
plt.rcParams["font.family"] = "DejaVu Sans"
CH = os.path.join(P["output_dir"], f"charts_{LBL['file']}"); os.makedirs(CH, exist_ok=True)

def fig_(w, h):
    f = plt.figure(figsize=(w, h), dpi=DPI); f.patch.set_facecolor(BG); return f
def clean(ax, grid="x"):
    ax.set_facecolor(BG)
    for sp in ["top", "right", "left"]: ax.spines[sp].set_visible(False)
    ax.spines["bottom"].set_color("#DDDDDD"); ax.grid(axis=grid, color=GRID, lw=0.6); ax.set_axisbelow(True)
    ax.tick_params(axis="y", length=0, labelsize=8, colors=INK); ax.tick_params(axis="x", labelsize=7.5, colors="#444444")
def head(f, t, s):
    f.text(0.5, 0.985, t, ha="center", va="top", fontsize=11.5, fontweight="bold", color=INK)
    f.text(0.5, 0.925, s, ha="center", va="top", fontsize=7.6, color=MUTE)
def kfmt(v): return "0" if v == 0 else (f"{v/1000:g}K" if v >= 1000 else f"{v:g}")
def xticks(ax, vmax, n=4):
    raw = max(vmax, 1) / n; mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw)
    t = [i * step for i in range(int(vmax // step) + 1)]
    ax.set_xticks(t); ax.set_xticklabels([kfmt(x) for x in t])
def save(f, name): f.savefig(os.path.join(CH, name), facecolor=BG); plt.close(f)

def make_charts(M, d):
    T, Z, NS, FL = d.T, d.Z, d.NS, d.FL
    # ---- c01 viewing intensity: two views
    f = fig_(9.4, 4.2); head(f, f"How Much Did the {K(T)} Churned Customers Watch?",
        f"Unique customers by viewing minutes  •  % of {K(T)}  •  churned subscription = the one that ended its {LBL['grace']} grace period in {LBL['short']}")
    ax = f.add_axes([0.09, 0.12, 0.88, 0.70]); clean(ax, "y"); ax.grid(axis="x", visible=False)
    x = np.arange(4); w = 0.36; a = [d.win[k] for k in BUCKETS]; b = [d.anyt[k] for k in BUCKETS]; vmax = max(a + b)
    b1 = ax.bar(x - w/2, a, w, color=TEAL, label=f"Viewing in the subscription that churned in {LBL['short']}")
    b2 = ax.bar(x + w/2, b, w, color=YEL, label="Any viewing, including earlier subscriptions that churned before")
    for r, v in list(zip(b1, a)) + list(zip(b2, b)):
        ax.text(r.get_x() + r.get_width()/2, v + vmax * 0.014, f"{K(v)}\n{pc(v, T)}", ha="center", va="bottom", fontsize=7.6, fontweight="bold", color=INK)
    ax.set_xticks(x); ax.set_xticklabels(["No viewing", "Up to 1 min", "1 to 10 min", "Over 10 min"], fontsize=8.5)
    ax.set_ylim(0, vmax * 1.16)
    t = [i * (20000 if vmax > 40000 else 5000) for i in range(int(vmax * 1.1 // (20000 if vmax > 40000 else 5000)) + 1)]
    ax.set_yticks(t); ax.set_yticklabels([kfmt(v) for v in t], fontsize=7.5)
    ax.legend(loc="upper left", fontsize=7.6, frameon=False, bbox_to_anchor=(0.0, 1.0)); ax.set_ylabel("Unique customers", fontsize=7.5, color=MUTE)
    save(f, "c01.png")

    # ---- c02 watching type
    f = fig_(9.4, 3.6); head(f, "Four Viewing Patterns Among Churned Customers",
        f"Was the viewing in the subscription that churned in {LBL['short']}, or in an earlier one that churned before?  •  {K(T)} unique customers")
    ax = f.add_axes([0.33, 0.12, 0.52, 0.68]); clean(ax)
    order = [("Watched in the churned subscription only", d.wt["Sub window only"], TEAL), ("Watched in the churned and an earlier one", d.wt["Both"], BLU),
             ("Watched only in an earlier subscription", d.wt["Outside window only"], YEL), ("No viewing at all", d.wt["No viewing"], ORG)]
    vmax = max(o[1] for o in order)
    for i, (n, v, c) in enumerate(order):
        ax.barh(i, v, color=c, height=0.62); ax.text(v + vmax * 0.012, i, f"{K(v)}  ({pc(v, T)})", va="center", fontsize=8.5, fontweight="bold", color=INK)
    ax.set_yticks(range(4)); ax.set_yticklabels([o[0] for o in order], fontsize=8.5); ax.invert_yaxis(); ax.set_xlim(0, vmax * 1.38); xticks(ax, vmax)
    save(f, "c02.png")

    # ---- c03 split of the zero-viewing group
    f = fig_(9.4, 3.4); head(f, f"From {K(T)} Churners to the {K(FL)} Who Tried and Failed",
        "Every churned customer, split by whether they watched, never opened a session, or opened sessions that never played")
    ax = f.add_axes([0.05, 0.40, 0.90, 0.20]); ax.set_facecolor(BG); ax.axis("off")
    left = 0
    for n, v, c in [(f"Watched something\n{K(T - Z)} ({pc(T - Z, T)})", T - Z, TEAL), (f"No sessions\n{K(NS)} ({pc(NS, T)})", NS, ORG), ("", FL, RED)]:
        ax.barh(0, v, left=left, color=c, height=0.9)
        if n: ax.text(left + v/2, 0, n, ha="center", va="center", fontsize=8.3, color="white", fontweight="bold")
        left += v
    ax.set_xlim(0, T); ax.set_ylim(-0.6, 0.6)
    f.text(0.955, 0.67, f"{K(FL)} ({pc(FL, T)})", ha="right", va="bottom", fontsize=8, color=RED, fontweight="bold")
    f.text(0.05, 0.67, f"All {K(T)} churned customers", ha="left", va="bottom", fontsize=8.5, color=INK, fontweight="bold")
    ax2 = f.add_axes([0.05, 0.10, 0.90, 0.16]); ax2.axis("off"); ax2.set_facecolor(BG); l2 = 0
    for n, v, c in [(f"{K(NS)} never started a session  ({pc(NS, Z)} of the {K(Z)})", NS, ORG), ("", FL, RED)]:
        ax2.barh(0, v, left=l2, color=c, height=0.9)
        if n: ax2.text(l2 + v/2, 0, n, ha="center", va="center", fontsize=8.3, color="white", fontweight="bold")
        l2 += v
    ax2.set_xlim(0, Z); ax2.set_ylim(-0.6, 0.6)
    f.text(0.05, 0.285, f"The {K(Z)} customers with no viewing at all", ha="left", va="bottom", fontsize=8.5, color=INK, fontweight="bold")
    f.text(0.955, 0.285, f"{K(FL)} tried; {K(M['raw_zero_play'])} of {K(M['raw_sessions'])} sessions had zero play time", ha="right", va="bottom", fontsize=8, color=RED, fontweight="bold")
    save(f, "c03.png")

    # ---- c04 login recency
    nlog = d.ld.get("Never logged in", 0)
    f = fig_(9.4, 3.9); head(f, f"When Did the {K(Z)} No-Viewing Customers Last Log In?",
        f"Days between last login and {LBL['cut']} {D_END.year}  •  customers (% of {K(Z)})  •  customers with no login date: {K(nlog)}")
    ax = f.add_axes([0.14, 0.12, 0.82, 0.68]); clean(ax)
    cmap = {"Never logged in": RED, "Within 7 days": GRN, "8-30 days": GRN, "31-90 days": BLU, "91-180 days": ORG, "180+ days": RED}
    ks = [k for k, _ in d.login]; vs = [v for _, v in d.login]; vmax = max(vs)
    for i, (k, v) in enumerate(zip(ks, vs)):
        ax.barh(i, v, color=cmap.get(k, GRY), height=0.62); ax.text(v + vmax * 0.012, i, f"{K(v)}  ({pc(v, Z)})", va="center", fontsize=8.5, fontweight="bold", color=INK)
    ax.set_yticks(range(len(ks))); ax.set_yticklabels(ks, fontsize=8.5); ax.invert_yaxis(); ax.set_xlim(0, vmax * 1.27); xticks(ax, vmax)
    old = d.ld.get("91-180 days", 0) + d.ld.get("180+ days", 0)
    ax.text(vmax * 1.24, min(1.4, len(ks) - 1.0), f"{K(old)} ({pc(old, Z)}) last logged in\nmore than 90 days before the cut-off", ha="right", va="center", fontsize=8, color=ORG, fontweight="bold")
    save(f, "c04.png")

    # ---- c05 country
    top = d.country[:12]; rest = Z - sum(v for _, v in top); names = [n for n, _ in top]; vals = [v for _, v in top]
    nrest = len(d.country) - len(top)
    if nrest > 0 and rest > 0: names.append(f"All other ({nrest} entries)"); vals.append(rest)
    top4 = sum(vals[:4])
    f = fig_(9.4, 4.2); head(f, f"Where Are the {K(Z)} No-Viewing Customers?",
        f"Customers by country (% of {K(Z)})  •  orange = the top 4 markets ({pc(top4, Z)})  •  top {len(top)} shown" + (f", {nrest} smaller entries combined" if nrest > 0 else ""))
    ax = f.add_axes([0.20, 0.10, 0.74, 0.74]); clean(ax)
    cc = [ORG] * min(4, len(top)) + [TEAL] * max(0, len(top) - 4) + ([GRY] if len(vals) > len(top) else [])
    vmax = max(vals)
    for i, (n, v, c) in enumerate(zip(names, vals, cc)):
        ax.barh(i, v, color=c, height=0.66); ax.text(v + vmax * 0.012, i, f"{K(v)}  ({pc(v, Z)})", va="center", fontsize=8, fontweight="bold", color=INK)
    ax.set_yticks(range(len(names))); ax.set_yticklabels(names, fontsize=8.3); ax.invert_yaxis(); ax.set_xlim(0, vmax * 1.27); xticks(ax, vmax)
    save(f, "c05.png")

    # ---- c06 payment channels
    topm = sorted(d.pay_total, key=lambda k: -d.pay_total[k])[:10]
    f = fig_(9.4, 4.3); head(f, "No-Viewing Customers by Payment Method",
        "Left: customers (dark = no sessions, red = sessions that failed)  •  Right: share who tried and failed, methods with 100+ customers")
    ax = f.add_axes([0.14, 0.10, 0.36, 0.74]); clean(ax); vmax = max(d.pay_total[k] for k in topm)
    for i, k in enumerate(topm):
        a, b = d.pay.get(k, (d.pay_total[k], 0)); ax.barh(i, a, color=TEAL, height=0.64); ax.barh(i, b, left=a, color=RED, height=0.64)
        ax.text(a + b + vmax * 0.012, i, K(d.pay_total[k]), va="center", fontsize=7.6, fontweight="bold", color=INK)
    ax.set_yticks(range(len(topm))); ax.set_yticklabels(topm, fontsize=8); ax.invert_yaxis(); ax.set_xlim(0, vmax * 1.22); xticks(ax, vmax, 3)
    ax.set_title("Customers", fontsize=8.5, fontweight="bold", color=INK, pad=5)
    big = [k for k in d.pay_total if d.pay_total[k] >= 100 and k in d.pay]; big.sort(key=lambda k: -d.pay[k][1] / d.pay_total[k])
    ax2 = f.add_axes([0.70, 0.10, 0.27, 0.74]); clean(ax2); rmax = max([d.pay[k][1] / d.pay_total[k] * 100 for k in big] + [1])
    for i, k in enumerate(big):
        r = d.pay[k][1] / d.pay_total[k] * 100
        ax2.barh(i, r, color=ORG if k in APP_PAYMENTS else TEAL, height=0.64); ax2.text(r + rmax * 0.02, i, f"{r:.1f}%", va="center", fontsize=7.6, fontweight="bold", color=INK)
    ax2.set_yticks(range(len(big))); ax2.set_yticklabels(big, fontsize=7.8); ax2.invert_yaxis(); ax2.set_xlim(0, rmax * 1.25)
    tk = [i * 5 for i in range(int(rmax * 1.2 // 5) + 1)]; ax2.set_xticks(tk); ax2.set_xticklabels([f"{t}%" for t in tk])
    allr = FL / Z * 100; ax2.axvline(allr, color="#E8636A", ls="--", lw=1); ax2.text(allr + rmax * 0.015, -0.45, f"All: {allr:.1f}%", color="#E8636A", fontsize=7, va="center")
    ax2.set_title("Tried and failed (% of customers)", fontsize=8.5, fontweight="bold", color=INK, pad=5)
    f.legend(handles=[Patch(color=ORG, label="App store, web, card, wallet"), Patch(color=TEAL, label="Telco / partner billing")], loc="lower right", fontsize=7.2, frameon=False, bbox_to_anchor=(0.985, 0.0), ncol=2)
    save(f, "c06.png")

    # ---- c07 subscription age and offer type
    f = fig_(9.4, 3.7); head(f, "Subscription Age and Offer Type of the No-Viewing Customers",
        f"Left: days from subscription start to {LBL['cut']} {D_END.year}  •  Right: offer type and direct / indirect sale")
    ax = f.add_axes([0.10, 0.12, 0.36, 0.68]); clean(ax)
    dk = [k for k, _, _, _ in M["dur"]]; dv = [n for _, n, _, _ in M["dur"]]; vmax = max(dv)
    for i, (k, v) in enumerate(zip(dk, dv)):
        ax.barh(i, v, color=ORG if v == vmax else TEAL, height=0.62); ax.text(v + vmax * 0.012, i, f"{K(v)}  ({pc(v, Z)})", va="center", fontsize=8, fontweight="bold", color=INK)
    ax.set_yticks(range(len(dk))); ax.set_yticklabels(dk, fontsize=8.5); ax.invert_yaxis(); ax.set_xlim(0, vmax * 1.42); xticks(ax, vmax, 3)
    ax.set_title("Subscription age", fontsize=8.5, fontweight="bold", color=INK, pad=5)
    ax2 = f.add_axes([0.63, 0.12, 0.34, 0.68]); clean(ax2)
    ol = [(f"{t} · {di}", n) for t, di, n in M["offer"]][:6]; vmax = max(n for _, n in ol); pal = [ORG, BLU, TEAL, GRY, YEL, GRN]
    for i, (n, v) in enumerate(ol):
        ax2.barh(i, v, color=pal[i % len(pal)], height=0.62); ax2.text(v + vmax * 0.012, i, f"{K(v)}  ({pc(v, d.offer_total)})", va="center", fontsize=8, fontweight="bold", color=INK)
    ax2.set_yticks(range(len(ol))); ax2.set_yticklabels([o[0] for o in ol], fontsize=8.3); ax2.invert_yaxis(); ax2.set_xlim(0, vmax * 1.6); xticks(ax2, vmax, 3)
    ax2.set_title("Offer type (customers can appear in more than one)", fontsize=8.5, fontweight="bold", color=INK, pad=5)
    save(f, "c07.png")

    # ---- c08 retry behaviour
    f = fig_(9.4, 3.8); head(f, f"How Hard Did the {K(FL)} Customers Try?",
        "Customers by number of session attempts  •  sessions in each group  •  share of sessions with zero play time")
    labs = [r[0] for r in d.retry]; cs = [r[1] for r in d.retry]; ss = [r[2] for r in d.retry]
    ax = f.add_axes([0.07, 0.14, 0.40, 0.64]); clean(ax, "y"); ax.grid(axis="x", visible=False)
    ax.bar(range(len(labs)), cs, color=ORG, width=0.62)
    for i, r in enumerate(d.retry): ax.text(i, r[1] + max(cs) * 0.02, f"{r[1]}\n({r[1]/FL*100:.0f}%)", ha="center", va="bottom", fontsize=8, fontweight="bold", color=INK)
    ax.set_xticks(range(len(labs))); ax.set_xticklabels(labs, fontsize=8); ax.set_ylim(0, max(cs) * 1.24); ax.set_yticks([])
    ax.set_title("Customers by attempts", fontsize=8.5, fontweight="bold", color=INK, pad=6)
    ax2 = f.add_axes([0.58, 0.14, 0.39, 0.64]); clean(ax2, "y"); ax2.grid(axis="x", visible=False)
    ax2.bar(range(len(labs)), ss, color=TEAL, width=0.62)
    for i, r in enumerate(d.retry): ax2.text(i, r[2] + max(ss) * 0.015, f"{K(r[2])}\n{r[3]/r[2]*100:.1f}% failed", ha="center", va="bottom", fontsize=7.6, fontweight="bold", color=INK)
    ax2.set_xticks(range(len(labs))); ax2.set_xticklabels(labs, fontsize=8); ax2.set_ylim(0, max(ss) * 1.28); ax2.set_yticks([])
    ax2.set_title(f"Sessions by attempt group ({K(sum(ss))} in total)", fontsize=8.5, fontweight="bold", color=INK, pad=6)
    save(f, "c08.png")

    # ---- c09 root causes
    f = fig_(9.4, 3.9); head(f, f"What Blocked the {K(FL)}? Customers by Error Type",
        "Primary error per customer (the error seen most often in their failed sessions)  •  customers; failed sessions of those customers")
    ax = f.add_axes([0.30, 0.12, 0.66, 0.68]); clean(ax)
    rc = [(k, v) for k, v in d.named] + ([("other", d.causes["other"])] if d.other_c else [])
    pal = {"silent": ORG, "entitlement": RED, "ios": BLU, "cdn": TEAL, "drm": YEL, "other": GRY}; vmax = max(v["customers"] for _, v in rc)
    for i, (k, v) in enumerate(rc):
        ax.barh(i, v["customers"], color=pal[k], height=0.62)
        ax.text(v["customers"] + vmax * 0.01, i, f"{K(v['customers'])}" + (f"  ({K(v['sessions'])} sessions)" if v["sessions"] else ""), va="center", fontsize=8, fontweight="bold", color=INK)
    ax.set_yticks(range(len(rc))); ax.set_yticklabels([v["chart"] for _, v in rc], fontsize=8); ax.invert_yaxis(); ax.set_xlim(0, vmax * 1.45); xticks(ax, vmax)
    save(f, "c09.png")

make_charts(M, d)
print("Charts saved to", CH)

# COMMAND ----------

# ---------------------------------------------------------------
# Deck helpers (python-pptx): TOD template drawn in code
# ---------------------------------------------------------------
from pptx import Presentation
from pptx.util import Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR, MSO_AUTO_SIZE
from pptx.oxml.ns import qn
from lxml import etree
from PIL import Image

LOGO_SMALL = "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"
LOGO_BIG = "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J8gA/LBtN9t//FuuuvOv/Or7UPmhzX6dpLlVW1l10b1ceNfj3NMYIuzWkMCNyT7TADMAsTixh2cy9YI7Gf/xJlY4iefongzs0I4jMtr1T/WB//p3njrrPzlr9qL0E1c20xAgS+zdz54pT3P30lUsvetK7j5lKCd/2/JeJhmpHslHq1n20EwemrmIF1pyQm6HEjphkJnWPxX7p2tZNfVlpj79Jk9+X1r95pQAssz85bxRvo4PrxnLxIljmdivZ+ocgARpVRzH6/ypGXJs2LSDLTNeY/q0v/PY3pqWH+1dkEM+Jq2X1FJLeyRgwzY2Pf0mT09/lb/srGRHK71im6cEkIWq6qh6YCa/nbmIWRNO44c/OYdrBvZiAD44MFHoditqkqz4CVizic//+jpPzFrEzM272ezyK7mraWnCAjsKH37Kh7MXMXv2ImZt2cMWj6PznBJAFtu6m80PzuLBWYuYNXIwp58xjDPOH8F5JUWUECCZBBKk3TtI3rInVQwgBpVVVC34mAWvf8gbb5bzxo59bHf9DbVUU2Vvipvk2f1f7GH7nHeYs3Q1SxYsY35Nozf3IWiLlACasSysplbusKXGrKbh3YTq1j1s3bqQp55ZyFNTOtLr7BMYPXIIpx3dnYFlHehanE9xsIDggZ7BtyWDZgv5kQai+2up3rGXHau+YE3553z06vvMW7eTda35PkwDM+3j0ZpvQIhAdS3VFVVUfLGHbcs2suL9VSxdtJK3KuuodD/i7KcEkGLb2Ot2sKEon+JYGvftMwwMbNhXw97WjO9wbdnDlsfmMe2xeUwrLaC0uIj2xfkUlRZRelQ3jhpzEuf07EjPRLM0Z4C5v57qN8p546O1fLRnPxW1DeyvrKFybwbf155q9qzZyNp4/PBmAUwwq+upnl/Om8s2sGxfDZVVtVTWNFBTWUNlY4TG1o5ZREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREREJH3/H/LrjDMiGYnzAAAAAElFTkSuQmCC"

INK_C, DARK_C, YEL_C, BLUE_C, GREY_C, WHITE_C, LIGHT_C, BORD_C = "1F1F1F", "262626", "FFBD00", "86A2D4", "8C8C8C", "FFFFFF", "F2F2F2", "D9D9D9"
FONT = "Arial"

def rgb(h): return RGBColor.from_string(h)
def _logo(name, b64):
    p = os.path.join(CH, name)
    with open(p, "wb") as fh: fh.write(base64.b64decode(b64))
    return p
LOGO_SMALL_P, LOGO_BIG_P = _logo("logo_small.png", LOGO_SMALL), _logo("logo_big.png", LOGO_BIG)

def new_deck():
    prs = Presentation()
    prs.slide_width, prs.slide_height = Pt(960), Pt(540)
    prs.core_properties.title = f"{LBL['long']} Churn Intelligence Report ({LBL['grace']} Grace Period)"
    prs.core_properties.author = "TOD AI & Data Analytics"
    return prs

def _alpha(el, pct_transparent):
    clr = el.find(qn("a:srgbClr"))
    if clr is not None:
        a = etree.SubElement(clr, qn("a:alpha")); a.set("val", str(int((100 - pct_transparent) * 1000)))

def shape(slide, kind, x, y, w, h, fill=None, fill_transp=0, line=None, line_w=0.75, line_transp=0, radius_pt=None, shadow=False, name=None):
    s = slide.shapes.add_shape(kind, Pt(x), Pt(y), Pt(w), Pt(h))
    st = s._element.find(qn("p:style"))
    if st is not None: s._element.remove(st)
    if radius_pt is not None and kind == MSO_SHAPE.ROUNDED_RECTANGLE: s.adjustments[0] = min(0.5, radius_pt / min(w, h))
    if fill: 
        s.fill.solid(); s.fill.fore_color.rgb = rgb(fill)
        if fill_transp: _alpha(s._element.spPr.find(qn("a:solidFill")), fill_transp)
    else: s.fill.background()
    if line:
        s.line.color.rgb = rgb(line); s.line.width = Pt(line_w)
        if line_transp: _alpha(s._element.spPr.find(qn("a:ln")).find(qn("a:solidFill")), line_transp)
    else: s.line.fill.background()
    if shadow:
        eff = etree.SubElement(s._element.spPr, qn("a:effectLst"))
        sh = etree.SubElement(eff, qn("a:outerShdw"), blurRad="50800", dist="25400", dir="2700000", algn="tl", rotWithShape="0")
        c = etree.SubElement(sh, qn("a:srgbClr"), val="000000"); etree.SubElement(c, qn("a:alpha")).set("val", "22000")
    if name: s.name = name
    return s

def poly(slide, pts, color, name):
    fb = slide.shapes.build_freeform(Pt(pts[0][0]), Pt(pts[0][1]))
    fb.add_line_segments([(Pt(a), Pt(b)) for a, b in pts[1:]], close=True)
    s = fb.convert_to_shape(); st = s._element.find(qn("p:style"))
    if st is not None: s._element.remove(st)
    s.fill.solid(); s.fill.fore_color.rgb = rgb(color); s.line.fill.background(); s.name = name
    return s

def _bullet(p, indent=11, numbered=False):
    pPr = p._p.get_or_add_pPr(); pPr.set("marL", str(int(Pt(indent)))); pPr.set("indent", str(-int(Pt(indent))))
    if numbered: etree.SubElement(pPr, qn("a:buAutoNum")).set("type", "arabicPeriod")
    else:
        etree.SubElement(pPr, qn("a:buFont")).set("typeface", "Arial"); etree.SubElement(pPr, qn("a:buChar")).set("char", "•")

def text(slide, x, y, w, h, content, size=12, bold=False, color=INK_C, align="l", valign="t", italic=False, spacing=None,
         space_after=None, bullets=False, numbered=False, name=None, char_spacing=None):
    """content: str | list of paragraphs; a paragraph is str or list of (text, {bold:..}) runs."""
    tb = slide.shapes.add_textbox(Pt(x), Pt(y), Pt(w), Pt(h)); tf = tb.text_frame
    tf.word_wrap = True; tf.auto_size = MSO_AUTO_SIZE.NONE
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    tf.vertical_anchor = {"t": MSO_ANCHOR.TOP, "m": MSO_ANCHOR.MIDDLE, "b": MSO_ANCHOR.BOTTOM}[valign]
    paras = content if isinstance(content, list) else [content]
    for i, para in enumerate(paras):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = {"l": PP_ALIGN.LEFT, "c": PP_ALIGN.CENTER, "r": PP_ALIGN.RIGHT}[align]
        if space_after is not None: p.space_after = Pt(space_after)
        if spacing: p.line_spacing = spacing
        runs = para if isinstance(para, list) else [(para, {})]
        for t, o in runs:
            r = p.add_run(); r.text = t; f = r.font; f.name = FONT; f.size = Pt(o.get("size", size))
            f.bold = o.get("bold", bold); f.italic = italic; f.color.rgb = rgb(o.get("color", color))
            if char_spacing: r._r.get_or_add_rPr().set("spc", str(int(char_spacing * 100)))
        if bullets or numbered: _bullet(p, 11 if bullets else 16, numbered)
    if name: tb.name = name
    return tb

def pic(slide, file, x, y, w, alt):
    path = os.path.join(CH, file); iw, ih = Image.open(path).size
    p = slide.shapes.add_picture(path, Pt(x), Pt(y), Pt(w), Pt(w * ih / iw)); p._element.nvPicPr.cNvPr.set("descr", alt); p.name = alt
    return p

def _footer(slide, number=True, dark=False):
    shape(slide, MSO_SHAPE.RECTANGLE, 0, 537, 960, 3.6, fill=YEL_C, name="Footer bar")
    slide.shapes.add_picture(LOGO_SMALL_P, Pt(868.1), Pt(496), Pt(57.9), Pt(24.2))
    if not dark:
        text(slide, 780, 526, 150, 10, "© TOD AI & Data Analytics", size=7, italic=True, color=YEL_C, align="r", name="Copyright")
        tb = text(slide, 914, 517, 40, 14, "", size=9, color=INK_C, align="r", name="Slide number")
        p = tb.text_frame.paragraphs[0]; fld = etree.SubElement(p._p, qn("a:fld"), id="{B6F15528-21DE-4FAA-801E-634DDDAF4B2B}", type="slidenum")
        rpr = etree.SubElement(fld, qn("a:rPr"), lang="en-US", sz="900"); sf = etree.SubElement(rpr, qn("a:solidFill")); etree.SubElement(sf, qn("a:srgbClr"), val=INK_C)
        etree.SubElement(rpr, qn("a:latin"), typeface=FONT); etree.SubElement(fld, qn("a:t")).text = "‹#›"

BLANK = None
def _slide(prs, bg):
    s = prs.slides.add_slide(prs.slide_layouts[6]); s.background.fill.solid(); s.background.fill.fore_color.rgb = rgb(bg); return s

def title_slide(prs, title, subtitle, l1, l2):
    s = _slide(prs, DARK_C); _footer(s, dark=True)
    s.shapes.add_picture(LOGO_BIG_P, Pt(386.2), Pt(75.6), Pt(192), Pt(192))
    text(s, 380, 513, 200, 14, "WWW.TOD.TV", size=8, color=WHITE_C, align="c")
    text(s, 40, 218, 880, 84, title.split("\n"), size=32, bold=True, color=YEL_C, align="c", char_spacing=4, name="Title")
    text(s, 180, 302, 600, 32, subtitle.split("\n"), size=11, color="D9D9D9", align="c", name="Subtitle")
    text(s, 180, 420, 600, 20, l1, size=14, bold=True, color=YEL_C, align="c", valign="m", char_spacing=4)
    text(s, 330, 443, 300, 22, l2, size=14, color=WHITE_C, align="c", valign="m")
    return s

def content(prs, title, kicker, bold_title=False, tab="CHURN ANALYSIS"):
    s = _slide(prs, WHITE_C)
    poly(s, [(188.9, 1.7), (875.2, 1.7), (875.2, 27.6), (167.8, 27.6)], BLUE_C, "Header bar")
    poly(s, [(0, 1.7), (182.7, 1.7), (161.6, 27.6), (0, 27.6)], YEL_C, "Header tab")
    text(s, 23.6, 5, 130, 20, tab, size=12, bold=True, color=WHITE_C, valign="m")
    text(s, 32.4, 34, 500, 14, kicker.upper(), size=8.5, color=GREY_C, valign="m", char_spacing=1)
    text(s, 32.4, 49, 800, 26, title, size=18, bold=bold_title, color=INK_C, name="Title")
    _footer(s)
    return s

def key_points(s, x, y, w, items, size=9):
    paras = [[("Key Points:", {"bold": True})]]
    tb = text(s, x, y, w, 380, "", size=size, name="Key points")
    tf = tb.text_frame; p0 = tf.paragraphs[0]; p0.space_after = Pt(6)
    r = p0.add_run(); r.text = "Key Points:"; r.font.bold = True; r.font.size = Pt(size); r.font.name = FONT; r.font.color.rgb = rgb(INK_C)
    for t in items:
        p = tf.add_paragraph(); p.space_after = Pt(5)
        r = p.add_run(); r.text = t; r.font.size = Pt(size); r.font.name = FONT; r.font.color.rgb = rgb(INK_C); _bullet(p, 11)

def note(s, x, y, w, h, t):
    shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h, fill=YEL_C, fill_transp=85, line=YEL_C, radius_pt=4.3, name="Data note box")
    text(s, x + 8, y, w - 16, h, [[("Data note: ", {"bold": True}), (t, {})]], size=9, valign="m", name="Data note")

def card(s, x, y, w, h, big, small):
    shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h, fill=YEL_C, fill_transp=78, line=GREY_C, line_transp=50, radius_pt=5.8, shadow=True, name="Card " + big)
    text(s, x, y + 8, w, 24, big, size=16, align="c", valign="m")
    text(s, x + 8, y + 32, w - 16, h - 36, small, size=12, align="c")

def badge(s, x, y, n):
    shape(s, MSO_SHAPE.OVAL, x, y, 26, 26, fill=YEL_C, line=YEL_C, line_w=0.5)
    text(s, x, y, 26, 26, str(n), size=13, bold=True, align="c", valign="m")

def findings(s, F):
    cw, ch, gx, gy, x0, y0 = 290, 178, 12, 12, 32.4, 86
    for i, (h, b) in enumerate(F):
        x, y = x0 + (i % 3) * (cw + gx), y0 + (i // 3) * (ch + gy)
        shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, x, y, cw, ch, fill=YEL_C, fill_transp=80, line=GREY_C, line_transp=50, radius_pt=5.8, shadow=True, name=f"Finding card {i+1}")
        badge(s, x + 12, y + 12, i + 1)
        text(s, x + 46, y + 10, cw - 56, 30, h, size=14, bold=True, valign="m")
        text(s, x + 14, y + 50, cw - 28, ch - 58, b, size=12.5)

def _cell_border(cell, color=BORD_C, w=0.5):
    tcPr = cell._tc.get_or_add_tcPr()
    for tag in ("a:lnL", "a:lnR", "a:lnT", "a:lnB"):
        ln = etree.SubElement(tcPr, qn(tag), w=str(int(Pt(w))), cap="flat", cmpd="sng", algn="ctr")
        sf = etree.SubElement(ln, qn("a:solidFill")); etree.SubElement(sf, qn("a:srgbClr"), val=color)
        etree.SubElement(ln, qn("a:prstDash"), val="solid")

def table(s, hdr, rows, colw, y, fs, x=32.4):
    n = len(rows) + 1; rh = fs * 1.25 + 9
    gf = s.shapes.add_table(n, len(hdr), Pt(x), Pt(y), Pt(sum(colw)), Pt(rh * n)); t = gf.table
    tblPr = t._tbl.tblPr; tblPr.set("firstRow", "0"); tblPr.set("bandRow", "0")
    sid = tblPr.find(qn("a:tableStyleId"))
    if sid is None: sid = etree.SubElement(tblPr, qn("a:tableStyleId"))
    sid.text = "{2D5ABB26-0587-4C30-8999-92F81FD0307C}"
    for j, wd in enumerate(colw): t.columns[j].width = Pt(wd)
    for i in range(n): t.rows[i].height = Pt(rh)
    for i in range(n):
        for j in range(len(hdr)):
            c = t.cell(i, j); val = hdr[j] if i == 0 else rows[i - 1][j]
            c.margin_left = c.margin_right = Pt(6); c.margin_top = c.margin_bottom = Pt(4); c.vertical_anchor = MSO_ANCHOR.MIDDLE
            _cell_border(c)
            c.fill.solid(); c.fill.fore_color.rgb = rgb(DARK_C if i == 0 else (LIGHT_C if i % 2 == 0 else WHITE_C))
            tf = c.text_frame; tf.word_wrap = True; p = tf.paragraphs[0]; r = p.add_run(); r.text = str(val)
            r.font.size = Pt(fs); r.font.name = FONT; r.font.bold = (i == 0) or (j == 0); r.font.color.rgb = rgb(YEL_C if i == 0 else INK_C)
    return gf

# COMMAND ----------

# ---------------------------------------------------------------
# Build the 16-slide deck from the metrics. Titles and key points are written from the numbers.
# ---------------------------------------------------------------
def listj(items):
    items = list(items)
    return items[0] if len(items) == 1 else ", ".join(items[:-1]) + " and " + items[-1]
NUMW = {1: "One", 2: "Two", 3: "Three", 4: "Four", 5: "Five", 6: "Six"}
cap = lambda s: s if (not s or s.startswith("iOS")) else s[0].upper() + s[1:]
def cstr(v, k=5): return ", ".join(f"{nice_country(c)} {n}" for c, n, _ in v["countries"][:k])
def cnames(v, k=5): return listj([nice_country(c) for c, _, _ in v["countries"][:k]])

def build_deck(M, d, path):
    T, S, Z, NSS, FL = d.T, d.S, d.Z, d.NS, d.FL
    win, anyt, wt = d.win, d.anyt, d.wt
    sh, lg, gr, cut = LBL["short"], LBL["long"], LBL["grace"], LBL["cut"]
    win_gt10, any_gt10 = win[">10 min"], anyt[">10 min"]
    low = anyt["0-1 min"] + anyt["1-10 min"]; win_none = win["No viewing"]
    sub_any = wt["Sub window only"] + wt["Both"]; earlier = wt["Outside window only"]
    ct = d.country; top4 = ct[:4]; top4_n = sum(n for _, n in top4); top2_n = sum(n for _, n in ct[:2])
    ld = d.ld; old = ld.get("91-180 days", 0) + ld.get("180+ days", 0); nlog = ld.get("Never logged in", 0)
    within30 = ld.get("Within 7 days", 0) + ld.get("8-30 days", 0)
    rt = {r[0]: r for r in d.retry}
    r1 = rt.get("1 attempt", ("", 0, 0, 0)); r23 = rt.get("2-3", ("", 0, 0, 0)); r410 = rt.get("4-10", ("", 0, 0, 0))
    n11 = rt.get("11-50", ("", 0, 0, 0))[1] + rt.get("50+", ("", 0, 0, 0))[1]; s11 = rt.get("11-50", ("", 0, 0, 0))[2] + rt.get("50+", ("", 0, 0, 0))[2]
    n50 = rt.get("50+", ("", 0, 0, 0))[1]
    rs, rz = M["raw_sessions"], M["raw_zero_play"]
    app_rate, ptn_rate = d.app_tried / max(d.app_n, 1) * 100, d.ptn_tried / max(d.ptn_n, 1) * 100
    top_app = max(d.app, key=lambda m: d.pay.get(m, (0, 0))[1]) if d.app else None
    ptn_txt = listj([f"{m} {K(d.pay_total[m])}" for m in d.top_ptn]); nptn = len(d.top_ptn)
    named, other_c = d.named, d.other_c
    first = named[0] if named else None
    hint = lambda m: f" ({PARTNER_HINT[m]})" if m in PARTNER_HINT else ""
    prs = new_deck()

    # 1. Title
    title_slide(prs, f"{LBL['upper']} CHURN\nINTELLIGENCE REPORT", "Deep Analysis of Who Churned, Who Never Watched\n& Why: the " + gr.title() + " Grace Period Cohort",
                f"CHURNED IN {LBL['upper']}", f"{gr.upper()} GRACE PERIOD")

    # 2. Executive summary
    kicker = f"Churn Report · {lg}"
    s = content(prs, f"Executive Summary — {pc(Z, T)} of Churners Never Watched; Most Went Silent", kicker, True)
    cause_txt = ""
    if named:
        parts = [f"{named[0][1]['plural']} hit {K(named[0][1]['customers'])} customers"] + [f"{v['plural']} {K(v['customers'])}" for _, v in named[1:]]
        cause_txt = cap(listj(parts)) + "."
    findings(s, [
        [f"{K(T)} customers churned", f"{K(T)} unique customers ({K(S)} subscriptions) reached the end of the {gr} grace period {LBL['inn']}. Most had used the service: {pc(win_gt10, T)} watched over 10 minutes during the subscription that churned."],
        ["One in five never watched" if 0.17 <= Z / T <= 0.23 else "Never watched", f"{K(Z)} customers ({pc(Z, T)}) have no viewing at all up to {cut}. Another {K(low)} ({pc(low, T)}) watched 10 minutes or less in total."],
        ["Most simply stopped logging in", f"{K(NSS)} of the {K(Z)} never started a session. {pc(old, Z)} of the {K(Z)} last logged in over 90 days before the cut-off, and " + ("none is missing a login date." if nlog == 0 else f"{K(nlog)} never logged in.")],
        [f"{NUMW.get(len(top4), len(top4))} countries, {NUMW.get(nptn, nptn).lower()} partner channels", f"{pc(top4_n, Z)} of the {K(Z)} sit in {listj([f'{n} ({pc(v, Z)})' for n, v in top4])}. {pc(d.top_ptn_n, Z)} were billed by {NUMW.get(nptn, nptn).lower()} telco or partner channels: {ptn_txt}."],
        [f"{K(FL)} tried and could not watch", f"{K(FL)} customers ({pc(FL, T)} of churners) opened {K(rs)} sessions and {K(rz)} had zero play time. {K(r1[1])} gave up after one try; {K(n11)} tried 11 times or more."],
        ["Named causes are fixable", f"{cause_txt} A further {K(other_c)} are not yet broken out."],
    ])
    text(s, 32.4, 466, 820, 26, [[("Reading this deck: ", {"bold": True}), (f"the zero-viewing story explains {pc(Z, T)} of churners. The other {pc(T - Z, T)} used the service at some point, and this deck does not cover why they left.", {})]], size=10.5, valign="m", name="Reading note")

    # 3. At a glance
    s = content(prs, f"Churn at a Glance: {K(T)} Customers, {pc(win_gt10, T)} Watched Over 10 Minutes", kicker, True)
    card(s, 64, 108, 182, 67, K(T), "Unique churned customers"); card(s, 269, 108, 182, 67, K(S), "Churned subscriptions")
    card(s, 474, 108, 182, 67, K(win_gt10), f"Watched over 10 min in the churned subscription ({pc(win_gt10, T)})")
    card(s, 684, 108, 182, 67, K(Z), f"No viewing at all ({pc(Z, T)})")
    card(s, 269, 195, 182, 67, K(NSS), f"No session of any kind ({pc(NSS, T)})"); card(s, 474, 195, 182, 67, K(FL), f"Tried but failed to play ({pc(FL, T)})")
    text(s, 54, 282, 850, 120, [f"The cohort: customers whose {gr} grace period ended {LBL['between']}",
         f"Viewing is checked two ways: in the subscription that churned in {sh}, and in any subscription including earlier ones",
         f"Only {pc(Z, T)} never watched; {pc(win_gt10, T)} watched over 10 minutes in the churned subscription",
         f"Of the {K(Z)} who never watched, {pc(NSS, Z)} never started a session and {pc(FL, Z)} tried and failed"],
         size=12, bold=True, color=GREY_C, bullets=True, space_after=6, name="Summary bullets")
    multi = f" {K(M['multi_start_customers'])} customers have subscriptions with more than one start date." if M.get("multi_start_customers") else ""
    note(s, 54, 420, 850, 56, f"churned means the {gr} grace period ended {LBL['between']}. Non-commercial, daily or weekly and promo subscriptions are excluded. The {K(T)} customers hold {K(S)} churned subscriptions;{multi}")

    # 4. Method
    s = content(prs, f"How It Was Built: {K(T)} Customers Matched to Viewing Sessions", kicker)
    steps = [("1", "Churn cohort", f"{K(T)} unique customers whose {gr} grace period ended {LBL['inn']}. Non-commercial, daily or weekly and promo subscriptions are excluded."),
             ("2", "Match viewing", f"Every customer is matched to the viewer-session table up to {cut}. Play time is summed to minutes per day; sessions with no asset or viewer ID are dropped."),
             ("3", "Two views", f"Churned-subscription view: viewing from the start date of the subscription that churned in {sh}. Any-viewing view: all viewing, including earlier subscriptions that churned before. Each is cut into 4 buckets."),
             ("4", "Deep dive", f"The {K(Z)} with no viewing are checked for raw sessions, last login, country, payment, offer, age and error codes.")]
    bw, gap, x0, y0 = 205, 25, 32.4, 92
    for i, (n, h, b) in enumerate(steps):
        x = x0 + i * (bw + gap)
        shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, x, y0, bw, 186, fill=YEL_C, fill_transp=80, line=GREY_C, line_transp=50, radius_pt=5.8, name="Step " + n)
        badge(s, x + 12, y0 + 12, n); text(s, x + 46, y0 + 10, bw - 54, 30, h, size=14, bold=True, valign="m"); text(s, x + 12, y0 + 50, bw - 24, 130, b, size=11.5)
        if i < 3: text(s, x + bw, y0 + 70, gap, 40, "›", size=28, bold=True, color=YEL_C, align="c", valign="m")
    text(s, 32.4, 296, 500, 20, "Viewing buckets (minutes watched)", size=13, bold=True)
    table(s, ["Bucket", "Rule", "Used in"], [["No viewing", "0 minutes", "Both views"], ["Up to 1 min", "More than 0 and up to 1 minute", "Both views"],
          ["1 to 10 min", "More than 1 and up to 10 minutes", "Both views"], ["Over 10 min", "More than 10 minutes", "Both views"]], [150, 400, 200], 320, 11)
    note(s, 32.4, 446, 895, 40, f"viewing comes from the viewer-session records. The churned-subscription view runs from its start date to {cut}, not to each customer's own expiry date.")

    # 5. Viewing intensity
    s = content(prs, f"{pc(win_gt10, T)} of Churners Watched Over 10 Minutes; {pc(Z, T)} Never Watched", "Viewing intensity")
    pic(s, "c01.png", 32.4, 84, 680, "Viewing minutes buckets chart")
    key_points(s, 728.9, 97, 205, [
        f"{K(win_gt10)} customers ({pc(win_gt10, T)}) watched over 10 minutes in the subscription that churned in {sh}; {K(any_gt10)} ({pc(any_gt10, T)}) did so in any subscription, including earlier ones",
        f"{K(win_none)} ({pc(win_none, T)}) had no viewing in the churned subscription, but only {K(Z)} ({pc(Z, T)}) had none at all",
        f"{K(low)} ({pc(low, T)}) watched 10 minutes or less in total", f"Low or no viewing at any time: {K(low + Z)} customers ({pc(low + Z, T)})"])
    note(s, 32.4, 398, 680, 50, f"\"churned subscription\" is the subscription that ended its {gr} grace period in {sh}. \"Any viewing\" totals all viewing, including subscriptions that churned earlier. Both views add to {K(T)}.")

    # 6. Viewing patterns
    s = content(prs, f"{pc(sub_any, T)} Watched in the Churned Subscription; {pc(earlier, T)} Only in an Earlier One", "Viewing intensity")
    pic(s, "c02.png", 32.4, 84, 680, "Watching type chart")
    key_points(s, 728.9, 97, 205, [
        f"{K(sub_any)} customers ({pc(sub_any, T)}) watched in the subscription that churned in {sh}: {K(wt['Sub window only'])} in it only and {K(wt['Both'])} in it and in an earlier subscription",
        f"{K(earlier)} ({pc(earlier, T)}) watched only in an earlier subscription that churned before, and not in the one that churned in {sh}",
        f"{K(Z)} ({pc(Z, T)}) have no viewing at all",
        f"Check: {K(win_none)} with no viewing in the churned subscription = {K(Z)} with none at all + {K(earlier)} earlier-only"])
    vs = dt.date.fromisoformat(P["viewing_start"])
    note(s, 32.4, 356, 680, 56, f"the churned subscription is the one that ended its {gr} grace period in {sh}. Viewing outside it is viewing dated before its start date, which belongs to an earlier subscription of the same customer that churned before. The data goes back to {vs:%b %Y}. Counts add to {K(T)}.")

    # 7. Split of the zero-viewing group
    s = content(prs, f"Of {K(Z)} Who Never Watched, {pc(NSS, Z)} Never Started a Session", "No-viewing deep dive")
    pic(s, "c03.png", 32.4, 84, 680, "Split of churned customers chart")
    key_points(s, 728.9, 97, 205, [f"{K(Z)} customers ({pc(Z, T)}) have no usable viewing",
        f"{K(NSS)} of them ({pc(NSS, Z)}; {pc(NSS, T)} of all churners) have no session of any kind in the viewing table",
        f"{K(FL)} ({pc(FL, Z)}; {pc(FL, T)} of churners) opened {K(rs)} sessions; {K(rz)} had zero or negative play time",
        f"Two different problems: {K(NSS)} disengaged, {K(FL)} were blocked"])
    ma = (f" {K(M['raw_missing_asset'])} sessions had no asset ID and carry {M['raw_missing_asset_mins']:g} minutes in total across {K(M['raw_missing_asset_viewers'])} customers; the other sessions carry none." if M["raw_missing_asset"] else "")
    note(s, 32.4, 342, 680, 56, f"the {K(FL)} are counted as \"no viewing\" because sessions with zero play time or no asset ID are excluded from minutes.{ma}")

    # 8. Login recency
    s = content(prs, f"{pc(old, Z)} of the No-Viewing Customers Had Not Logged In for 90+ Days", "No-viewing deep dive")
    pic(s, "c04.png", 32.4, 84, 680, "Last login recency chart")
    pts = [f"{K(ld.get('91-180 days', 0))} ({pc(ld.get('91-180 days', 0), Z)}) last logged in 91 to 180 days before {cut} and {K(ld.get('180+ days', 0))} ({pc(ld.get('180+ days', 0), Z)}) over 180 days before: {K(old)} ({pc(old, Z)}) together",
           f"Only {K(within30)} ({pc(within30, Z)}) logged in within 30 days of the cut-off; {K(ld.get('Within 7 days', 0))} within 7 days"]
    pts.append("No customer has a missing login date: the \"never logged in\" bucket is empty. These customers logged in once, then stopped" if nlog == 0 else f"{K(nlog)} customers ({pc(nlog, Z)}) never logged in")
    key_points(s, 728.9, 97, 205, pts)
    note(s, 32.4, 378, 680, 56, f"the login figures cover all {K(Z)}, including the {K(FL)} who tried to play. Login problems themselves are not measured in this data.")

    # 9. Country
    s = content(prs, f"{NUMW.get(len(top4), len(top4))} Markets Hold {pc(top4_n, Z, 0)} of No-Viewing Customers; {ct[0][0]} Alone {pc(ct[0][1], Z, 0)}", "No-viewing deep dive")
    pic(s, "c05.png", 32.4, 84, 680, "No-viewing customers by country chart")
    nxt = ct[4:6]
    key_points(s, 728.9, 97, 205, [
        f"{ct[0][0]} has {K(ct[0][1])} ({pc(ct[0][1], Z)}) and {ct[1][0]} {K(ct[1][1])} ({pc(ct[1][1], Z)}): {pc(top2_n, Z)} together",
        f"{listj([f'{n} ({K(v)}, {pc(v, Z)})' for n, v in ct[2:4]])} bring the top four to {pc(top4_n, Z)}",
        (f"{nxt[0][0]} has {K(nxt[0][1])} ({pc(nxt[0][1], Z)})" + (f" and {nxt[1][0]} {K(nxt[1][1])} ({pc(nxt[1][1], Z)})" if len(nxt) > 1 else "")) if nxt else "Other markets are small",
        f"The other {max(len(ct) - 4, 0)} entries hold {pc(Z - top4_n, Z)} together"])
    note(s, 32.4, 398, 680, 46, f"counts are for the {K(Z)} only. Churners by country for all {K(T)} are not part of this analysis, so zero-viewing rates by country, and whether a market over-indexes, cannot be calculated yet.")

    # 10. Payment channel
    s = content(prs, f"Partner Billing Is {pc(d.top_ptn_n, Z, 0)} of No-Viewers; App and Card Payers Fail More Often" if app_rate > ptn_rate else f"Partner Billing Is {pc(d.top_ptn_n, Z, 0)} of No-Viewers", "No-viewing deep dive")
    pic(s, "c06.png", 32.4, 84, 680, "Payment method chart")
    pk = [f"{NUMW.get(nptn, nptn)} telco and partner channels bill {K(d.top_ptn_n)} ({pc(d.top_ptn_n, Z)}) of the {K(Z)}: " + listj([f"{m}{hint(m)} {K(d.pay_total[m])}" for m in d.top_ptn]),
          f"Partner and telco billing: {ptn_rate:.1f}% tried and failed ({K(d.ptn_tried)} of {K(d.ptn_n)})",
          f"App store, web, card and wallet: {app_rate:.1f}% tried and failed ({K(d.app_tried)} of {K(d.app_n)})"]
    if top_app: pk.append(f"{top_app} alone has {K(d.pay[top_app][1])} of the {K(FL)} ({pc(d.pay[top_app][1], FL, 0)})")
    key_points(s, 728.9, 97, 205, pk)
    nos = sum(a for a, _ in d.pay.values())
    small = [(m, d.pay[m][1] / d.pay_total[m] * 100, d.pay_total[m]) for m in d.pay_total if m in d.pay and 20 <= d.pay_total[m] < 100 and d.pay[m][1] / d.pay_total[m] * 100 > app_rate]
    sm = (f" {max(small, key=lambda t: t[1])[0]} ({max(small, key=lambda t: t[1])[1]:.1f}%) rests on {K(max(small, key=lambda t: t[1])[2])} customers." if small else "")
    note(s, 32.4, 406, 680, 60, f"payment methods are grouped by name: {listj(d.app[:7])} versus telco and partner billing. Customers with more than one method count in each ({K(nos)} in the no-session group against {K(NSS)}).{sm}")

    # 11. Subscription age and offer
    dur = M["dur"]; bd = max(dur, key=lambda t: t[1]); others = sorted([t for t in dur if t is not bd], key=lambda t: -t[1])
    ind = sum(n for _, di, n in M["offer"] if di.lower() == "indirect"); dire = d.offer_total - ind
    if bd[3] < 1000:
        a_ = D_END - dt.timedelta(days=bd[3]); b_ = D_END - dt.timedelta(days=bd[2]); span = f", roughly between {a_.day} {a_:%b} and {b_.day} {b_:%b}"
        phrase = f"{bd[2]} to {bd[3]} Days"
    else: span = ""; phrase = f"Over {bd[2] - 1} Days"
    s = content(prs, f"{pc(bd[1], Z, 0)} of the No-Viewing Customers Started {phrase} Before the Cut-Off", "No-viewing deep dive")
    pic(s, "c07.png", 32.4, 84, 680, "Subscription age and offer type chart")
    wave = bd[1] / Z > 0.5 and ind / max(d.offer_total, 1) > 0.5
    key_points(s, 728.9, 97, 205, [f"{K(bd[1])} ({pc(bd[1], Z)}) started {bd[0].replace('-', ' to ').replace('days', 'days')} before {cut}{span}",
        "Other ages: " + ", ".join(f"{k} {K(n)} ({pc(n, Z)})" for k, n, _, _ in others),
        f"Indirect sales are {K(ind)} of {K(d.offer_total)} offers ({pc(ind, d.offer_total)}); direct sales {K(dire)} ({pc(dire, d.offer_total)})"] +
        (["A single acquisition wave through partners fits this pattern but is not proven"] if wave else []))
    multi_n = f"customers with more than one subscription appear in more than one bucket (age counts add to {K(d.dur_total)} and offer counts to {K(d.offer_total)}, against {K(Z)})." if (d.dur_total != Z or d.offer_total != Z) else "each customer appears once in each view."
    note(s, 32.4, 362, 680, 56, multi_n + (" The wave idea is a hypothesis; partner campaign dates are needed to test it." if wave else ""))

    # 12. Retry behaviour
    f1 = M["startup"].get("1", (0, 0)); f0 = M["startup"].get("0", (0, 0))
    s = content(prs, f"Half of Failed Sessions Come From {K(n11)} Customers Who Retried 11+ Times" if s11 / max(rs, 1) >= 0.45 else f"{K(n11)} Customers Retried 11+ Times and Made {pc(s11, rs, 0)} of Failed Sessions", "Playback failures")
    pic(s, "c08.png", 32.4, 84, 680, "Retry behaviour chart")
    key_points(s, 728.9, 97, 205, [f"{K(FL)} customers opened {K(rs)} sessions; {K(rz)} ({pc(rz, rs)}) had zero play time",
        f"{K(r1[1])} ({pc(r1[1], FL, 0)}) tried once, {K(r23[1])} ({pc(r23[1], FL, 0)}) two or three times, {K(r410[1])} ({pc(r410[1], FL, 0)}) four to ten times",
        f"{K(n11)} customers ({pc(n11, FL, 0)}) tried 11 or more times; they account for {K(s11)} sessions ({pc(s11, rs)})",
        f"{K(f1[0])} failed sessions ({pc(f1[0], rz)}) carry a startup error flag, across {K(f1[1])} customers"])
    note(s, 32.4, 370, 680, 46, f"startup error flag 1 covers {K(f1[0])} sessions and {K(f1[1])} customers; flag 0 covers {K(f0[0])} sessions and {K(f0[1])} customers. Customers can appear in both groups. {NUMW.get(n50, n50)} customers tried more than 50 times.")

    # 13. Root-cause chart
    s = content(prs, (f"{first[1]['title']} Are the Largest Known Cause: {K(first[1]['customers'])} of {K(FL)} Customers" if first else f"Playback Failures: {K(FL)} Customers"), "Playback failures")
    pic(s, "c09.png", 32.4, 84, 680, "Root cause chart")
    kp = []
    for k, v in named[:2]:
        kp.append(f"{v['label']}: {K(v['customers'])} customers and {K(v['sessions'])} sessions ({cstr(v, 5)})" + (f"; {v['tail']}" if v["tail"] else ""))
    rest = [f"{v['label']} {K(v['customers'])}" for _, v in named[2:]]
    if rest: kp.append(listj(rest))
    kp.append(f"{K(other_c)} customers ({pc(other_c, FL, 0)}) have other errors not broken out")
    key_points(s, 728.9, 97, 205, kp)
    note(s, 32.4, 378, 680, 62, "each customer is assigned the error they hit most often in their failed sessions; the rest fall in the \"other errors\" group. Detailed error lists by device, CDN and asset are not part of this view.")

    # 14. Root-cause table
    ordr = [k for k in ["entitlement", "silent", "ios", "cdn", "drm"] if k in d.causes and d.causes[k]["customers"] > 0]
    ent = d.causes.get("entitlement")
    t14 = (f"Entitlement Denials Hit Paying Subscribers in {listj([nice_country(c) for c, _, _ in ent['countries'][:4]])}" if ent and ent["customers"] > 0 and ent["countries"] else "What Customers Saw When Playback Failed, and What to Check First")
    s = content(prs, t14, "Playback failures")
    rows = [[d.causes[k]["label"], d.causes[k]["saw"], cstr(d.causes[k], 4), K(d.causes[k]["customers"]), K(d.causes[k]["sessions"]), d.causes[k]["check"]] for k in ordr]
    table(s, ["Cause", "What the customer saw", "Countries (customers)", "Cust.", "Sessions", "First check"], rows, [88, 215, 185, 40, 66, 301], 92, 11)
    rep = []
    for k in ordr:
        for c, n, ss_ in d.causes[k]["countries"]:
            if n and ss_ >= 50 and ss_ / n >= 10: rep.append((ss_ / n, nice_country(c), n, ss_, d.causes[k]["label"]))
    extra = ""
    if rep:
        _, c_, n_, s_, l_ = max(rep); extra = f" In {c_}, {n_} customers ({l_}) made {s_} failed sessions (about {round(s_ / n_)} each), which points to a repeating device- or account-specific problem rather than a wide outage."
    note(s, 32.4, 92 + (len(rows) + 1) * 37 + 14, 895, 52, "causes and first checks are indicative readings of each error and have not yet been confirmed with engineering." + extra)

    # 15. Segment summary
    s = content(prs, f"Only {pc(FL, T)} of Churners Tried and Failed; {pc(NSS, T)} Went Silent", "Playback failures")
    seg = [["Dormant, no session", K(NSS), pc(NSS, Z, 2), pc(NSS, T, 2), "Disengagement; no session opened", "Strategic"]]
    for k, v in named: seg.append([v["label"], K(v["customers"]), pc(v["customers"], Z, 2), pc(v["customers"], T, 2), v["root"], v["prio"]])
    if other_c: seg.append(["Tried, other errors", K(other_c), pc(other_c, Z, 2), pc(other_c, T, 2), "Not yet broken out", "To analyse"])
    seg.append(["Total no viewing", K(Z), "100%", pc(Z, T, 2), "", ""])
    table(s, ["Segment", "Customers", f"% of {K(Z)}", f"% of {K(T)}", "Root cause", "Priority"], seg, [160, 85, 95, 105, 320, 130], 92, 12.5)
    note(s, 32.4, 92 + (len(seg) + 1) * (12.5 * 1.25 + 9) + 14, 895, 46, "priority is an assessment based on customers affected and severity: paying customers denied access rank highest.")

    # 16. Recommendations
    s = content(prs, f"Recommendations: Fix the {K(FL)} Blocked, Re-engage the {K(NSS)} Silent", "Actions", True)
    C = d.causes
    def has(k): return k in C and C[k]["customers"] > 0
    left = []
    if has("entitlement"): left.append(f"Audit the billing-to-entitlement sync: {K(C['entitlement']['customers'])} subscribers in {cnames(C['entitlement'], 4)} were told to upgrade")
    if has("silent"): left.append(f"Add error capture to the player: {K(C['silent']['customers'])} silent failures in {cnames(C['silent'], 5)} leave no trace")
    cd_ = []
    if has("cdn"): cd_.append(f"Review CDN geo rules for {cnames(C['cdn'], 3)}")
    if has("drm"): cd_.append(f"the DRM licence set-up for {cnames(C['drm'], 3)}")
    if cd_: left.append(", and ".join(cd_) if len(cd_) > 1 else cd_[0])
    if has("ios"): left.append(f"Test the current iOS build for the -1102 error ({cnames(C['ios'], 4)})")
    if other_c: left.append(f"Break out the {K(other_c)} customers whose errors are not yet classified")
    right = [f"Trigger a re-engagement message when a subscriber has not logged in for 14 days; {pc(old, Z)} of the {K(Z)} were silent for 90 days or more",
             f"Add a first-view journey for partner-billed customers: {NUMW.get(nptn, nptn).lower()} partner channels hold {pc(d.top_ptn_n, Z)} of the no-viewing group",
             f"Focus on {ct[0][0]} and {ct[1][0]}, which hold {pc(top2_n, Z)}",
             f"Review the {gr} grace period: it keeps customers on the books long after they stopped logging in",
             f"Study the {K(win_gt10)} engaged churners separately: they are the larger loss and the data here cannot say why they left"]
    for x, ttl, sub, items in [(32.4, f"Fix the blockers: the {K(FL)}", "Technical, start now", left), (487.6, f"Re-engage the silent: the {K(NSS)}", "Engagement, plan next", right)]:
        shape(s, MSO_SHAPE.ROUNDED_RECTANGLE, x, 88, 440, 372, fill=YEL_C, fill_transp=85, line=GREY_C, line_transp=50, radius_pt=4.3)
        text(s, x + 14, 96, 412, 22, ttl, size=14, bold=True); text(s, x + 14, 118, 412, 18, sub, size=10.5, color=GREY_C)
        text(s, x + 14, 144, 412, 306, items, size=13, numbered=True, space_after=10, name="Actions")
    prs.save(path)
    return path

# COMMAND ----------

# ---------------------------------------------------------------
# Build the deck, save it, and print where it is
# ---------------------------------------------------------------
fname = f"TOD_Churn_{LBL['file']}_Intelligence_Report" + ("_SAMPLE" if P["use_sample"] else "") + ".pptx"
out_path = build_deck(M, d, os.path.join(P["output_dir"], fname))
print("Deck saved:", out_path)
if out_path.startswith("/dbfs/FileStore/"):
    rel = out_path[len("/dbfs/FileStore/"):]
    try:
        displayHTML(f'<p>Download: <a href="/files/{rel}" target="_blank">{fname}</a> (open from your workspace host: https://&lt;workspace-host&gt;/files/{rel})</p>')
    except NameError:
        pass
failed = [n for n, ok in M.get("checks", []) if not ok]
print("Reconciliation checks:", "all passed" if not failed else f"FAILED: {failed}")
