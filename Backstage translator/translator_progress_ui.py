from __future__ import annotations

from typing import Callable, Optional, Tuple

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)


console = Console()


class ProgressManager:
    def __init__(self) -> None:
        self.progress = Progress(
            SpinnerColumn(style="cyan"),
            TextColumn("{task.description}", style="bold"),
            BarColumn(bar_width=None),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            expand=True,
            transient=False,
        )
        self.avg_progress = Progress(
            TextColumn("Średnia samoocena:"),
            BarColumn(bar_width=None, complete_style="green"),
            TextColumn("{task.completed:.2f}/5"),
            expand=True,
            transient=False,
        )
        self._task_main: Optional[int] = None
        self._task_avg: Optional[int] = None

    def start(self) -> None:
        self.progress.start()
        self.avg_progress.start()
        self._task_avg = self.avg_progress.add_task("avg", total=5.0, completed=0.0)

    def stop(self) -> None:
        self.progress.stop()
        self.avg_progress.stop()

    def new_task(self, description: str, total: int) -> int:
        if self._task_main is not None:
            self.progress.remove_task(self._task_main)
        self._task_main = self.progress.add_task(description, total=total)
        return self._task_main

    def advance_main(self, step: int = 1) -> None:
        if self._task_main is not None:
            self.progress.update(self._task_main, advance=step)

    def update_main(self, completed: int, total: Optional[int] = None) -> None:
        if self._task_main is not None:
            kwargs = {"completed": completed}
            if total is not None:
                kwargs["total"] = total
            self.progress.update(self._task_main, **kwargs)

    def update_avg(self, avg_value: float) -> None:
        if self._task_avg is not None:
            self.avg_progress.update(self._task_avg, completed=max(0.0, min(5.0, avg_value)))

    # Callbacks for pipeline
    def make_phase_callbacks(self, total_results: int = 0) -> Tuple[
        Callable[[str, int, int], None], Callable[[float], None]
    ]:
        # on_progress(event, current, total), on_avg_update(avg)
        def on_progress(event: str, current: int, total: int) -> None:
            if event == "prepare_requests":
                self.new_task("Przygotowanie requestów…", total)
                self.update_main(current, total)
            elif event == "upload":
                self.new_task("Upload batch…", total)
                self.update_main(current, total)
            elif event == "poll":
                self.new_task("Polling status…", total)
                # keep as spinner-like: just set total=0 handled by caller
            elif event == "download":
                self.new_task("Pobieranie wyników…", total)
                self.update_main(current, total)
            elif event == "process_results":
                if self._task_main is None:
                    self.new_task("Przetwarzanie wyników…", total)
                self.update_main(current, total)
            elif event == "selection":
                self.new_task("Selekcja do weryfikacji…", total)
                self.update_main(current, total)
            elif event == "verify_upload":
                self.new_task("Weryfikacja – upload…", total)
                self.update_main(current, total)
            elif event == "verify_process":
                self.new_task("Weryfikacja – przetwarzanie wyników…", total)
                self.update_main(current, total)
            elif event == "update_csv":
                self.new_task("Aktualizacja CSV…", total)
                self.update_main(current, total)

        def on_avg_update(avg: float) -> None:
            self.update_avg(avg)

        return on_progress, on_avg_update


def print_config_summary(
    path: str,
    sep: str,
    total: int,
    key_col: Tuple[int, str],
    en_col: Tuple[int, str],
    pl_col: Tuple[int, str],
    to_translate: int,
    placeholders: int,
    sample: object,
    est_cost_min: float,
    est_cost_max: float,
    est_cost_avg: float,
) -> None:
    console.print(
        f"Plik: {path}\nSeparator: '{sep}'\nWiersze: {total}\n"
        f"Kolumny: key[{key_col[0]}:{key_col[1]}], EN[{en_col[0]}:{en_col[1]}], PL[{pl_col[0]}:{pl_col[1]}]\n"
        f"Do tłumaczenia: {to_translate}, z placeholderami: {placeholders}\n"
        f"Przykładowe linie: {sample}\n"
        f"Szacunkowy koszt (z cachingiem): ${est_cost_min:.2f} - ${est_cost_max:.2f} (śr. ${est_cost_avg:.2f})"
    )

