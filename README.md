I'll build a Databricks notebook that runs the churn analysis and generates a deck like this one. I'm starting from the logic in your original notebook.

I'm building a Databricks notebook that runs the churn analysis and generates the same deck. I've read your original notebook and the deck code, and I'm now writing the notebook. I'll test it locally on synthetic data before sending it.

The data-loading and analysis cells of the notebook are written. I'm now writing the chart and deck cells, then I'll test the deck output against the existing deck.

The notebook's charts now render from the embedded September figures. I'm checking them visually against the existing deck, then I'll write the slide-building cell.

The notebook now produces all 16 slides in the sample run, and the layout looks close to your current deck. I'm fixing some small differences (table spacing, a note-box position, a few wording points), then I'll package the notebook.

I've built a Databricks notebook that runs the churn analysis and produces the same 16-slide deck. I haven't run the Spark queries, so the first real run is the real test.

- **What I tested:** In sample mode the notebook built all 16 slides from the September 2026 figures, with no database access. The deck passed validation and the layout matches your current one. The Spark cells (cohort, viewing, deep dive) couldn't be tested here because Spark can't be installed in this workspace.
- **What it does:** It reads the churn cohort and the viewer sessions, then writes the deck, the charts and a file with every number used. The queries follow your original notebook's logic, rewritten in PySpark. Titles and key points are written from the computed numbers, so a new month needs no manual editing. It prints reconciliation checks, such as buckets adding to 113,610.
- **Setting it up:** Import it with Workspace > Import. Before running, change `secret_scope` and the two secret key names to the ones that hold your Synapse login. The other widgets (period, grace days, paths, output folder) default to September 2026 and your original paths. Run it with `use_sample_metrics = true` first to check the template and the output folder.
- **Subscription count:** I set the 116,294 total to count churned subscription rows. Your original notebook output showed 115,146, which was the distinct count. I'm inferring 116,294 is the row count. The run prints both, so please compare it against 116,294.
- **Root causes now cover every customer:** The notebook assigns each of the 815 the error they hit most often, so the 465 "not broken out" will shrink. The cause counts may differ from the deck, which used only the top error per country.
- **Viewing scope:** The session query reads the whole `ViewerSessions` view, which does not exclude sports or live content. That probably removes the "sports not covered" doubt I flagged earlier. The query still limits it to the listed MENA countries and to valid asset IDs.

Files are in /tmp/build:
- TOD_Churn_Report_Builder.py
