from latch.types import LatchAuthor, LatchMetadata, LatchParameter

metadata = LatchMetadata(
    display_name="NextClade",
    documentation="https://github.com/jvfe/nextclade_latch/blob/main/README.md",
    author=LatchAuthor(
        name="jvfe",
        github="https://github.com/jvfe",
    ),
    repository="https://github.com/jvfe/nextclade_latch",
    license="MIT",
    tags=["NGS", "virus", "phylogenetics"],
)

metadata.parameters = {
    "samples": LatchParameter(
        display_name="NextClade samples (FASTA files)",
        description="FASTA files with viral sequences.",
        batch_table_column=True,
        section_title="Data",
    ),
    "database": LatchParameter(
        display_name="NextClade Database",
        description="Available NextClade databases to align to",
    ),
}
