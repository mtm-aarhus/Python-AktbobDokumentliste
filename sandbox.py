"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement, QueueStatus

from robot_framework.process import process
from robot_framework.initialize import initialize
import os
import json
from typing import Optional

def make_queue_element_with_payload(
    payload: dict | list,
    queue_name: str,
    reference: Optional[str] = None,
    created_by: Optional[str] = None,
    status: QueueStatus = QueueStatus.NEW, 
) -> QueueElement:
    # Validate & serialize
    data_str = json.dumps(payload, ensure_ascii=False)
    if len(data_str) > 2000:
        raise ValueError("data exceeds 2000 chars (column limit)")

    return QueueElement(
        queue_name=queue_name,
        status=status,
        data=data_str,
        reference=reference,
        created_by=created_by,
    )

# pylint: disable-next=unused-argum
orchestrator_connection = OrchestratorConnection(
    "AktbobDokumentliste",
    os.getenv("OpenOrchestratorSQL"),
    os.getenv("OpenOrchestratorKey"),
    None,
)

qe = make_queue_element_with_payload(
    payload={
        "SagsNummer": "GEO-2013-026501",
    },
    queue_name="AktbobDokumentlisteQueue",
    reference="Sandbox",
    status=QueueStatus.NEW, 
)


process(orchestrator_connection, qe)
