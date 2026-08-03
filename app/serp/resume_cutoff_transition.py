from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Mapping

from app.common.exceptions import CriticalPipelineError
from app.serp.collection_plan import CollectionPlanBundle, CollectionRuntimeWindow


SCHEMA_VERSION = "wb_resume_cutoff_transition_v1"
TRANSITION_ID = "wb-20260801-183812Z-successor-2300-2359-v2"
COORDINATOR_RUN_ID = "nightly-20260801-49b1d183812f"
PREDECESSOR_COORDINATOR_RUN_ID = "nightly-20260801-66cbc819ed0f"
COORDINATOR_STAGE = "wb_resume"
MATRIX_RUN_ID = "20260801_183812Z"
COLLECTION_RUN_ID = MATRIX_RUN_ID
EXECUTION_MATRIX_ID = "four-region-nightly-v1"
EXECUTION_MATRIX_SHA256 = (
    "54a9f3872b88475be606e19037d0fa91a7b8c7470cb57d7c05ce7108f8951da7"
)
COLLECTION_PLAN_ID = "shevron-four-regions-top1000-v2"
COLLECTION_PLAN_SHA256 = (
    "17ea62bc6ce5fe0419b63a399bf0138d38c83a02d915fb3b8b55577ea89935a7"
)
QUERY_PACK_SHA256 = (
    "412c5be528b0258f9f27dd7020e211b5452ac0eb001ff98cd26077a70dd9a02f"
)
REGION_REGISTRY_SHA256 = (
    "b52692d462e2f334ed74d821bf5f1f6f7a3c1bb21812630a3aa372e08a0f935d"
)
EFFECTIVE_PLAN_SHA256 = (
    "0b37ed2c84f50a79f69f89c769ee3823055bbea9394f56d1baa1b7e77d03443e"
)
FROM_CUTOFF_MSK = "23:00"
TO_CUTOFF_MSK = "23:59"
FROM_DEADLINE_UTC = datetime(2026, 8, 1, 20, 0, tzinfo=UTC)
TO_DEADLINE_UTC = datetime(2026, 8, 1, 20, 59, tzinfo=UTC)

STORED_TRANSPORT_FINGERPRINT = {
    "schema_version": "wb_transport_fingerprint_v1",
    "ordered_endpoint_urls_sha256": (
        "6adec51ee15afaf98b7b2a66b53b735fcb8149f0d42b71b3bfd3e5ccc8a6ae08"
    ),
    "request_params_sha256": (
        "740688b67b86ba24e9130bba4e0813b4c00176f1f53c6990dfa9465020de1714"
    ),
    "proxy_route_sha256": (
        "632d1832fa9c70611097b15f2ce2754c492de1b4dcbfb161cc3d54e1e3c8df44"
    ),
    "input_manifest_sha256": (
        "ae7780b166a4be2945792286042c918029b88a71f186e135909c95eb87c2a925"
    ),
    "runtime_input_sha256": (
        "49bdfdd7d575f6289d653b6f65d91e63295a23afadca511ba255dc84d87756a6"
    ),
    "fingerprint_sha256": (
        "7bcd2d7b3c4d4ff4b5a925b2b2d9015bc5083659607a93bb6ff63012294b5021"
    ),
}

_OLD_RUNTIME_WINDOW = CollectionRuntimeWindow(
    mode="bounded_resumable",
    scheduled_start_msk="00:15",
    new_run_start_grace_seconds=81900,
    max_invocation_runtime_seconds=21600,
    absolute_cutoff_msk=FROM_CUTOFF_MSK,
    minimum_resume_window_seconds=1800,
    finalization_reserve_seconds=60,
)
_SHA256_FIELDS = {
    "execution_matrix_sha256",
    "collection_plan_sha256",
    "query_pack_sha256",
    "region_registry_sha256",
    "effective_plan_sha256",
    "from_transport_fingerprint_sha256",
    "to_transport_fingerprint_sha256",
    "runtime_input_sha256",
    "from_input_manifest_sha256",
    "to_input_manifest_sha256",
}
_SAFE_TRANSITION_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}")


class ResumeCutoffTransitionError(CriticalPipelineError):
    pass


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8") + b"\n"


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _transport_fingerprint_sha256(value: Mapping[str, Any]) -> str:
    payload = {
        key: item
        for key, item in value.items()
        if key != "fingerprint_sha256"
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _utc_iso(value: datetime) -> str:
    return (
        value.astimezone(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _fingerprint(value: Any, *, field: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ResumeCutoffTransitionError(f"{field} is invalid")
    normalized = dict(value)
    if normalized != STORED_TRANSPORT_FINGERPRINT:
        raise ResumeCutoffTransitionError(f"{field} identity mismatch")
    return normalized


@dataclass(frozen=True, slots=True)
class ApprovedResumeCutoffTransition:
    transition_id: str = TRANSITION_ID

    def __post_init__(self) -> None:
        if self.transition_id != TRANSITION_ID:
            raise ResumeCutoffTransitionError(
                "resume cutoff transition ID mismatch"
            )

    def validate_invocation(
        self,
        *,
        run_id: str,
        resume: bool,
        absolute_deadline_utc: datetime | None,
    ) -> None:
        if (
            run_id != MATRIX_RUN_ID
            or not resume
            or absolute_deadline_utc != TO_DEADLINE_UTC
        ):
            raise ResumeCutoffTransitionError(
                "resume cutoff transition invocation mismatch"
            )

    def validate_bundle(self, bundle: CollectionPlanBundle) -> None:
        plan = bundle.collection_plan
        if (
            plan.collection_plan_id != COLLECTION_PLAN_ID
            or bundle.collection_plan_sha256 != COLLECTION_PLAN_SHA256
            or bundle.query_pack_sha256 != QUERY_PACK_SHA256
            or bundle.region_registry_sha256 != REGION_REGISTRY_SHA256
            or plan.runtime_window != _OLD_RUNTIME_WINDOW
        ):
            raise ResumeCutoffTransitionError(
                "resume cutoff transition source identity mismatch"
            )

    def validate_matrix(self, matrix: Any) -> None:
        if (
            getattr(matrix, "execution_matrix_id", None)
            != EXECUTION_MATRIX_ID
            or getattr(matrix, "source_sha256", None)
            != EXECUTION_MATRIX_SHA256
        ):
            raise ResumeCutoffTransitionError(
                "resume cutoff transition matrix identity mismatch"
            )
        entries = tuple(getattr(matrix, "enabled_entries", ()))
        if len(entries) != 1:
            raise ResumeCutoffTransitionError(
                "resume cutoff transition matrix scope mismatch"
            )
        self.validate_bundle(entries[0].bundle)

    def runtime_window(
        self,
        window: CollectionRuntimeWindow,
    ) -> CollectionRuntimeWindow:
        if window != _OLD_RUNTIME_WINDOW:
            raise ResumeCutoffTransitionError(
                "resume cutoff transition runtime window mismatch"
            )
        return replace(window, absolute_cutoff_msk=TO_CUTOFF_MSK)

    def authorization_evidence(self) -> dict[str, Any]:
        evidence: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "transition_id": self.transition_id,
            "authorization_scope": "exact_resume_only",
            "deadline_scope": "same_day_collection_and_downstream",
            "coordinator_run_id": COORDINATOR_RUN_ID,
            "predecessor_coordinator_run_id": (
                PREDECESSOR_COORDINATOR_RUN_ID
            ),
            "coordinator_stage": COORDINATOR_STAGE,
            "matrix_run_id": MATRIX_RUN_ID,
            "collection_run_id": COLLECTION_RUN_ID,
            "execution_matrix_id": EXECUTION_MATRIX_ID,
            "execution_matrix_sha256": EXECUTION_MATRIX_SHA256,
            "collection_plan_id": COLLECTION_PLAN_ID,
            "collection_plan_sha256": COLLECTION_PLAN_SHA256,
            "query_pack_sha256": QUERY_PACK_SHA256,
            "region_registry_sha256": REGION_REGISTRY_SHA256,
            "effective_plan_sha256": EFFECTIVE_PLAN_SHA256,
            "from_cutoff_msk": FROM_CUTOFF_MSK,
            "to_cutoff_msk": TO_CUTOFF_MSK,
            "from_deadline_utc": _utc_iso(FROM_DEADLINE_UTC),
            "to_deadline_utc": _utc_iso(TO_DEADLINE_UTC),
            "from_transport_fingerprint_sha256": (
                STORED_TRANSPORT_FINGERPRINT["fingerprint_sha256"]
            ),
            "runtime_input_sha256": (
                STORED_TRANSPORT_FINGERPRINT["runtime_input_sha256"]
            ),
            "from_input_manifest_sha256": (
                STORED_TRANSPORT_FINGERPRINT["input_manifest_sha256"]
            ),
        }
        evidence["evidence_sha256"] = _sha256(_canonical_bytes(evidence))
        return evidence

    def validated_evidence(
        self,
        *,
        bundle: CollectionPlanBundle,
        effective_plan_sha256: str,
        prior_manifest: Mapping[str, Any],
        effective_plan: Mapping[str, Any],
        current_transport_fingerprint: Mapping[str, Any],
        attestation_transition: Mapping[str, Any] | None,
        allow_completed: bool = False,
    ) -> dict[str, Any]:
        self.validate_bundle(bundle)
        if effective_plan_sha256 != EFFECTIVE_PLAN_SHA256:
            raise ResumeCutoffTransitionError(
                "resume cutoff transition effective plan mismatch"
            )
        allowed_statuses = {
            "running",
            "failed",
            "publication_pending",
        }
        if allow_completed:
            allowed_statuses.add("success")
        if (
            prior_manifest.get("run_id") != COLLECTION_RUN_ID
            or (
                prior_manifest.get("complete") is True
                and not allow_completed
            )
            or prior_manifest.get("status") not in allowed_statuses
        ):
            raise ResumeCutoffTransitionError(
                "resume cutoff transition prior state mismatch"
            )
        for source, field in (
            (prior_manifest, "resume manifest"),
            (effective_plan, "effective plan"),
        ):
            for key, expected in {
                "collection_plan_id": COLLECTION_PLAN_ID,
                "collection_plan_sha256": COLLECTION_PLAN_SHA256,
                "query_pack_sha256": QUERY_PACK_SHA256,
                "region_registry_sha256": REGION_REGISTRY_SHA256,
                "effective_plan_sha256": EFFECTIVE_PLAN_SHA256,
            }.items():
                if key == "effective_plan_sha256" and field == "effective plan":
                    continue
                if source.get(key) != expected:
                    raise ResumeCutoffTransitionError(
                        f"resume cutoff transition {field} mismatch"
                    )
        _fingerprint(
            effective_plan.get("transport_fingerprint"),
            field="effective plan transport fingerprint",
        )
        expected_runtime = {
            "mode": _OLD_RUNTIME_WINDOW.mode,
            "scheduled_start_msk": _OLD_RUNTIME_WINDOW.scheduled_start_msk,
            "new_run_start_grace_seconds": (
                _OLD_RUNTIME_WINDOW.new_run_start_grace_seconds
            ),
            "max_invocation_runtime_seconds": (
                _OLD_RUNTIME_WINDOW.max_invocation_runtime_seconds
            ),
            "absolute_cutoff_msk": _OLD_RUNTIME_WINDOW.absolute_cutoff_msk,
            "minimum_resume_window_seconds": (
                _OLD_RUNTIME_WINDOW.minimum_resume_window_seconds
            ),
            "finalization_reserve_seconds": (
                _OLD_RUNTIME_WINDOW.finalization_reserve_seconds
            ),
        }
        if effective_plan.get("runtime_window") != expected_runtime:
            raise ResumeCutoffTransitionError(
                "resume cutoff transition effective runtime mismatch"
            )

        current = dict(current_transport_fingerprint)
        if (
            set(current) != set(STORED_TRANSPORT_FINGERPRINT)
            or current.get("fingerprint_sha256")
            != _transport_fingerprint_sha256(current)
        ):
            raise ResumeCutoffTransitionError(
                "resume cutoff transition current fingerprint is invalid"
            )
        for key in (
            "schema_version",
            "ordered_endpoint_urls_sha256",
            "request_params_sha256",
            "proxy_route_sha256",
            "runtime_input_sha256",
        ):
            if current.get(key) != STORED_TRANSPORT_FINGERPRINT[key]:
                raise ResumeCutoffTransitionError(
                    "resume cutoff transition current transport mismatch"
                )
        prior_fingerprint = prior_manifest.get("transport_fingerprint")
        if prior_fingerprint not in (
            STORED_TRANSPORT_FINGERPRINT,
            current,
        ):
            raise ResumeCutoffTransitionError(
                "resume cutoff transition manifest transport mismatch"
            )
        if not isinstance(attestation_transition, Mapping):
            raise ResumeCutoffTransitionError(
                "resume cutoff transition input attestation is missing"
            )
        if (
            attestation_transition.get("schema_version")
            != "wb_resume_attestation_transition_v1"
            or attestation_transition.get("transition_id")
            != TRANSITION_ID
            or attestation_transition.get("from_input_manifest_sha256")
            != STORED_TRANSPORT_FINGERPRINT["input_manifest_sha256"]
            or attestation_transition.get("to_input_manifest_sha256")
            != current.get("input_manifest_sha256")
            or attestation_transition.get(
                "from_transport_fingerprint_sha256"
            )
            != STORED_TRANSPORT_FINGERPRINT["fingerprint_sha256"]
            or attestation_transition.get("to_transport_fingerprint_sha256")
            != current.get("fingerprint_sha256")
        ):
            raise ResumeCutoffTransitionError(
                "resume cutoff transition input attestation mismatch"
            )
        evidence = self.authorization_evidence()
        evidence.pop("evidence_sha256")
        evidence.update(
            {
                "validation_status": "validated_before_resume_network",
                "to_transport_fingerprint_sha256": current.get(
                    "fingerprint_sha256"
                ),
                "to_input_manifest_sha256": current.get(
                    "input_manifest_sha256"
                ),
            }
        )
        for field in _SHA256_FIELDS:
            value = evidence.get(field)
            if value is not None and (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ResumeCutoffTransitionError(
                    "resume cutoff transition evidence hash is invalid"
                )
        evidence["evidence_sha256"] = _sha256(_canonical_bytes(evidence))
        return evidence


def resolve_resume_cutoff_transition(
    *,
    run_id: str,
    resume: bool,
    coordinator_run_id: str,
    coordinator_stage: str,
    transition_id: str,
    absolute_deadline_utc: datetime | None,
    validated_coordinator_authority: bool = False,
) -> ApprovedResumeCutoffTransition | None:
    if run_id != MATRIX_RUN_ID:
        if not transition_id:
            return None
        if (
            transition_id == TRANSITION_ID
            or not _SAFE_TRANSITION_ID.fullmatch(transition_id)
            or not validated_coordinator_authority
            or not resume
            or not coordinator_run_id
            or coordinator_stage not in {"wb_initial", "wb_resume"}
            or absolute_deadline_utc is None
        ):
            raise ResumeCutoffTransitionError(
                "unrelated transition metadata is not authorized"
            )
        # A validated coordinator may carry successor metadata for another
        # marketplace. It grants no WB cutoff authority and is ignored here;
        # ordinary post-cutover deadline, lock and publication gates still apply.
        return None
    if (
        not resume
        or coordinator_run_id != COORDINATOR_RUN_ID
        or coordinator_stage != COORDINATOR_STAGE
        or transition_id != TRANSITION_ID
        or absolute_deadline_utc != TO_DEADLINE_UTC
    ):
        raise ResumeCutoffTransitionError(
            "exact resume cutoff transition authorization mismatch"
        )
    return ApprovedResumeCutoffTransition()


def canonical_transition_bytes(value: Mapping[str, Any]) -> bytes:
    return _canonical_bytes(value)
