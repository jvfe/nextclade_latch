import re
import subprocess
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Tuple

from dataclasses_json import dataclass_json
from flytekit import task
from flytekitplugins.pod import Pod
from kubernetes.client.models import (
    V1Container,
    V1PodSpec,
    V1ResourceRequirements,
    V1Toleration,
)
from latch import map_task, message, small_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import LatchDir, LatchFile

from .docs import metadata


@dataclass_json
@dataclass
class Sample:
    name: str
    fasta: LatchFile


class Database(Enum):
    sars_cov_2 = "sars-cov-2"
    MPXV = "MPXV"
    hMPXV = "hMPXV"
    hMPXV_B1 = "hMPXV_B1"
    flu_h1n1pdm_ha = "flu_h1n1pdm_ha"
    flu_h3n2_ha = "flu_h3n2_ha"
    flu_vic_ha = "flu_vic_ha"
    flu_yam_ha = "flu_yam_ha"


# From: https://github.com/latch-verified/bulk-rnaseq/blob/64a25531e1ddc43be0afffbde91af03754fb7c8c/wf/__init__.py
def _get_96_spot_pod() -> Pod:
    """[ "c6i.24xlarge", "c5.24xlarge", "c5.metal", "c5d.24xlarge", "c5d.metal" ]"""

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": "90", "memory": "170Gi"},
        limits={"cpu": "96", "memory": "192Gi"},
    )
    primary_container.resources = resources

    return Pod(
        pod_spec=V1PodSpec(
            containers=[primary_container],
            tolerations=[
                V1Toleration(effect="NoSchedule", key="ng", value="cpu-96-spot")
            ],
        ),
        primary_container_name="primary",
    )


large_spot_task = task(task_config=_get_96_spot_pod(), retries=3)


def _capture_output(command: List[str]) -> Tuple[int, str]:
    captured_stdout = []

    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            print(line)
            captured_stdout.append(line)
        process.wait()
        returncode = process.returncode

    return returncode, "\n".join(captured_stdout)


@small_task
def get_database(database_name: Database) -> LatchDir:
    """Get Nextclade database"""

    database_outname = f"nextclade_database/{database_name.value}"
    database_outdir = Path(database_outname).resolve()

    _nextclade_cmd = [
        "nextclade",
        "dataset",
        "get",
        "--name",
        database_name.value,
        "--output-dir",
        database_outname,
    ]

    message(
        "info",
        {
            "title": f"Downloading NextClade database {database_name.value}",
            "body": f"Running command: {''.join(_nextclade_cmd)}",
        },
    )

    subprocess.run(_nextclade_cmd)

    return LatchDir(str(database_outdir), f"latch:///{database_outname}/")


@dataclass_json
@dataclass
class NextcladeInput:
    name: str
    fasta: LatchFile
    database: LatchDir


@small_task
def prepare_nextclade_inputs(
    samples: List[Sample], database: LatchDir
) -> List[NextcladeInput]:

    inputs = []
    for sample in samples:
        cur_input = NextcladeInput(
            name=sample.name, fasta=sample.fasta, database=database
        )
        inputs.append(cur_input)

    return inputs


@large_spot_task
def run_nextclade(nc_input: NextcladeInput) -> LatchDir:

    output_dirname = nc_input.name
    remote_path = f"latch:///nextclade_outputs/{output_dirname}"
    output_dir = Path(output_dirname).resolve()

    _nextclade_cmd = [
        "nextclade",
        "run",
        "--input-dataset",
        nc_input.database.local_path,
        "--output-all",
        output_dirname,
        nc_input.fasta.local_path,
    ]

    return_code, stdout = _capture_output(_nextclade_cmd)

    running_cmd = " ".join(_nextclade_cmd)

    message(
        "info",
        {
            "title": f"Executing NextClade for {nc_input.name}",
            "body": running_cmd,
        },
    )

    if return_code != 0:
        errors = re.findall("Message.*", stdout[1])
        for error in errors:
            message(
                "error",
                {
                    "title": f"An error was raised while running NextClade for {nc_input.name}:",
                    "body": error,
                },
            )
        raise RuntimeError

    return LatchDir(str(output_dir), remote_path)


@workflow(metadata)
def nextclade(
    samples: List[Sample], database_name: Database = Database.sars_cov_2
) -> List[LatchDir]:
    """Analysis of viral genetic sequences

    Nextclade
    ---

    Nextclade is an open-source project for viral genome alignment,
    mutation calling, clade assignment, quality checks and phylogenetic placement.
    """

    database = get_database(database_name=database_name)
    nextclade_inputs = prepare_nextclade_inputs(samples=samples, database=database)
    return map_task(run_nextclade)(nc_input=nextclade_inputs)


LaunchPlan(
    nextclade,
    "FASTA files with viral sequences",
    {
        "samples": [
            Sample(
                name="cluster_cov",
                fasta=LatchFile("s3://latch-public/test-data/4318/cluster_cov.fasta"),
            ),
            Sample(
                name="sars_sequences",
                fasta=LatchFile(
                    "s3://latch-public/test-data/4318/sars_sequences.fasta"
                ),
            ),
        ],
        "database_name": Database.sars_cov_2,
    },
)
