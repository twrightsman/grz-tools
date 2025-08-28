import itertools
import logging
from operator import itemgetter

import rich.align
import rich.pretty
import rich.table
import sqlalchemy.orm
import textual
import textual.logging
from cryptography.hazmat.primitives.serialization import SSHPublicKeyTypes
from grz_db.models.submission import Submission, SubmissionDb, SubmissionStateLog
from sqlalchemy import func as sqlfn
from sqlmodel import select
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import HorizontalScroll
from textual.widgets import DataTable, Footer, Static, TabbedContent, TabPane

logger = logging.getLogger(__name__)
logger.addHandler(textual.logging.TextualHandler())


class SubmissionCountByStateTable(Static):
    def on_mount(self) -> None:
        self.loading = True

    @textual.work
    async def load(self, database: SubmissionDb) -> None:
        with database._get_session() as session:
            # credit: https://stackoverflow.com/a/28090544
            log1 = sqlalchemy.orm.aliased(SubmissionStateLog, name="log1")
            log2 = sqlalchemy.orm.aliased(SubmissionStateLog, name="log2")

            statement = (
                select(log1.state, sqlfn.count(log1.state))
                .join(
                    log2, (log1.submission_id == log2.submission_id) & (log1.timestamp < log2.timestamp), isouter=True
                )
                .where(log2.timestamp.is_(None))
                .group_by(log1.state)
            )

            counts_by_state = session.exec(statement).all()

        table = rich.table.Table(
            rich.table.Column(header="State"),
            rich.table.Column(header="Submission Count", justify="center"),
            show_header=False,
        )
        total = 0
        for state, count in counts_by_state:
            table.add_row(str(state), rich.pretty.Pretty(count))
            total += count
        table.add_section()
        table.add_row("[bold]Total[/bold]", rich.pretty.Pretty(total))
        self.border_title = "Submission count by state"
        self.styles.border = ("round", self.app.theme_variables["foreground"])
        self.styles.min_width = 31
        self.update(rich.align.Align.center(table))
        self.loading = False


class SubmissionCountByConsentTable(Static):
    def on_mount(self) -> None:
        self.loading = True

    @textual.work
    async def load(self, database: SubmissionDb) -> None:
        with database._get_session() as session:
            statement = select(Submission.consented, sqlfn.count(Submission.consented)).group_by(Submission.consented)
            counts_by_consent = session.exec(statement).all()

        table = rich.table.Table(
            rich.table.Column(header="Consented"),
            rich.table.Column(header="Submission Count", justify="center"),
            show_header=False,
        )
        for consented, count in counts_by_consent:
            table.add_row(rich.pretty.Pretty(consented), rich.pretty.Pretty(count))
        self.border_title = "Submission count by consent"
        self.styles.border = ("round", self.app.theme_variables["foreground"])
        self.styles.min_width = 33
        self.update(rich.align.Align.center(table))
        self.loading = False


class SubmissionCountByDetailedQCByLETable(Static):
    def on_mount(self) -> None:
        self.loading = True

    @textual.work
    async def load(self, database: SubmissionDb) -> None:
        with database._get_session() as session:
            statement = select(
                Submission.submitter_id,
                Submission.detailed_qc_passed,
                sqlfn.count(Submission.submitter_id),  # can't count detailed_qc_pass because NULLs are not counted
            ).group_by(Submission.submitter_id, Submission.detailed_qc_passed)
            counts_by_pass_by_le = session.exec(statement).all()

        rows = []
        for submitter_id, group in itertools.groupby(
            sorted(counts_by_pass_by_le, key=itemgetter(0)), key=itemgetter(0)
        ):
            qced = 0
            total = 0
            for g in group:
                if g[1]:
                    qced += g[2]
                total += g[2]
            rows.append([submitter_id, qced, total])

        table = rich.table.Table(
            rich.table.Column(header="Submitter ID"), rich.table.Column(header="QCed", justify="center")
        )
        for submitter_id, qced, total in rows:
            table.add_row(str(submitter_id), f"{qced}/{total} ({qced / total:.1%})")
        self.border_title = "Detailed QC by LE"
        self.styles.border = ("round", self.app.theme_variables["foreground"])
        self.update(rich.align.Align.center(table))
        self.loading = False


class DatabaseBrowser(App):
    """A Textual app to browse a GRZ submission database."""

    BINDINGS = [
        Binding(key="q", action="quit", description="Quit the app"),
    ]
    CSS_PATH = "tui.tcss"

    def __init__(self, database: SubmissionDb, public_keys: dict[str, SSHPublicKeyTypes], **kwargs) -> None:
        super().__init__(**kwargs)
        self._database = database
        self._public_keys = public_keys

    def compose(self) -> ComposeResult:
        with TabbedContent():
            with TabPane("Overview", id="pane-overview"):
                with HorizontalScroll(id="container-overview-charts"):
                    yield SubmissionCountByStateTable()
                    yield SubmissionCountByConsentTable()
                    yield SubmissionCountByDetailedQCByLETable()
                yield DataTable(id="table-states-latest")
            with TabPane("Issues", id="pane-issues"):
                yield DataTable(id="table-issues")
            with TabPane("Search", id="pane-search"):
                yield HorizontalScroll(id="container-search-inputs")
                yield DataTable(id="table-search-results")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(SubmissionCountByStateTable).load(self._database)
        self.query_one(SubmissionCountByConsentTable).load(self._database)
        self.query_one(SubmissionCountByDetailedQCByLETable).load(self._database)
