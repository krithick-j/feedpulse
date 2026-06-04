from __future__ import annotations

from dataclasses import dataclass


@dataclass
class WorkflowTaskInput:
    task_id: int
    url: str
    queue: str


@dataclass
class ProcessXmlJobInput:
    job_id: str
    tasks: list[WorkflowTaskInput]


@dataclass
class ProcessedTaskResult:
    task_id: int
    status: str
    queue: str
    records_extracted: int
