from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .durable_atomic import durable_atomic_copy
from .nightly_attestation import integrity_gate


@dataclass(slots=True)
class ProjectPaths:
    BASE_DIR: Path
    DATA_DIR: Path
    RAW_DIR: Path
    STAGING_DIR: Path
    MARTS_DIR: Path
    LOG_DIR: Path
    EXPORTS_DIR: Path
    STATE_DIR: Path
    SQLITE_DIR: Path
    CHECKPOINT_DIR: Path
    SQLITE_DB: Path

    @classmethod
    def from_config(
        cls,
        project_root: Path,
        *,
        data_raw: Path,
        data_staging: Path,
        data_marts: Path,
        logs: Path,
        exports: Path,
        state_sqlite: Path,
        checkpoints_dir: Path,
    ) -> "ProjectPaths":
        data_dir = data_raw.parent
        state_dir = state_sqlite.parent.parent
        return cls(
            BASE_DIR=project_root,
            DATA_DIR=data_dir,
            RAW_DIR=data_raw,
            STAGING_DIR=data_staging,
            MARTS_DIR=data_marts,
            LOG_DIR=logs,
            EXPORTS_DIR=exports,
            STATE_DIR=state_dir,
            SQLITE_DIR=state_sqlite.parent,
            CHECKPOINT_DIR=checkpoints_dir,
            SQLITE_DB=state_sqlite,
        )

    def ensure_base_dirs(self) -> None:
        for folder in [
            self.DATA_DIR,
            self.RAW_DIR,
            self.STAGING_DIR,
            self.MARTS_DIR,
            self.LOG_DIR,
            self.EXPORTS_DIR,
            self.STATE_DIR,
            self.SQLITE_DIR,
            self.CHECKPOINT_DIR,
        ]:
            folder.mkdir(parents=True, exist_ok=True)

    def _layer_root(self, layer: str) -> Path:
        if layer == "raw":
            return self.RAW_DIR
        if layer == "staging":
            return self.STAGING_DIR
        if layer == "marts":
            return self.MARTS_DIR
        raise ValueError(f"Unsupported layer: {layer}")

    def layer_component_run_dir(self, layer: str, component: str, run_id: str) -> Path:
        path = self._layer_root(layer) / component / run_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def output_path(self, *, layer: str, component: str, run_id: str, filename: str) -> Path:
        return self.layer_component_run_dir(layer=layer, component=component, run_id=run_id) / filename

    def latest_output_path(self, *, layer: str, component: str, filename: str) -> Path | None:
        comp_dir = self._layer_root(layer) / component
        if not comp_dir.exists():
            return None

        latest_candidate = comp_dir / "latest" / filename
        if latest_candidate.exists():
            return latest_candidate

        run_dirs = sorted(
            [p for p in comp_dir.iterdir() if p.is_dir() and p.name != "latest"],
            reverse=True,
        )
        for run_dir in run_dirs:
            candidate = run_dir / filename
            if candidate.exists():
                return candidate
        return None

    def publish_latest_output(self, *, layer: str, component: str, source_path: Path, filename: str) -> Path:
        latest_dir = self._layer_root(layer) / component / "latest"
        latest_dir.mkdir(parents=True, exist_ok=True)
        target = latest_dir / filename
        durable_atomic_copy(
            source_path.absolute(),
            target.absolute(),
            integrity_gate=integrity_gate(self.BASE_DIR),
        )
        return target

    def publish_output_copy(
        self,
        *,
        source_path: Path,
        target_path: Path,
    ) -> Path:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        durable_atomic_copy(
            source_path.absolute(),
            target_path.absolute(),
            integrity_gate=integrity_gate(self.BASE_DIR),
        )
        return target_path
