# Data Preparation Tool (`data-prep`)

This command-line tool is designed to clean and transform raw JSON data into a flattened format suitable for search
indexing, such as with Google Cloud Vertex AI Search. It can process data files and generate the corresponding flattened
JSON schemas.

## Workflows & Usage

There are two primary workflows: generating a flattened schema and processing a data file.

### Workflow 1: Generating a Flattened Schema

Use this workflow to see what your new, flattened schema will look like without processing any data. This is useful for
designing and validating your indexing strategy.

**Command:**

```bash
data-prep schema [OPTIONS] <SOURCE_SCHEMA_FILE>
```

**Example:** Generate a new schema by flattening nested objects and arrays, and save it to the `output` directory with
logs.

```bash
data-prep schema examples/customer_schema.json --flat-deep --flat-array --output-dir output --log
```

### Workflow 2: Processing Data

Use this workflow to clean and flatten your raw JSON data file based on the rules defined by the flags.

**Command:**

```bash
data-prep process [OPTIONS] <SCHEMA_FILE> <INPUT_DATA_FILE>
```

**Example:** Process a data file by flattening nested objects and arrays, saving the cleaned data and logs to the
`output` directory.

```bash
data-prep process examples/customer_schema.json examples/customer_sample_data.json --flat-deep --flat-array --output-dir output --log
```

---

## File Naming Convention

When using the `--output-dir` option, the tool will generate files with the following naming convention to keep runs
organized:

`[source_file_name]_[type]_[timestamp].[ext]`

- **`[source_file_name]`**: The base name of the input file (e.g., `customer_schema` or `customer_sample_data`).
- **`[type]`**: The type of output file (`schema`, `data`, or `log`).
- **`[timestamp]`**: A 6-digit timestamp (`HHMMSS`) that is shared for all files generated in a single run.
- **`[ext]`**: The file extension (`.json` or `.log`).

**Example Output Files:**

- `customer_sample_data_data_143210.json`
- `customer_sample_data_log_143210.log`

---

## Options

| Option         | Description                                                                                            |
| -------------- | ------------------------------------------------------------------------------------------------------ |
| `--flat-deep`  | Recursively flattens nested objects, joining keys with an underscore (e.g., `a.b` becomes `a_b`).      |
| `--flat-array` | Flattens arrays of strings into a single, space-separated string (e.g., `["a", "b"]` becomes `"a b"`). |
| `--output-dir` | The directory where output files will be saved. If not provided, the schema will be printed to stdout. |
| `--log`        | Enables detailed logging to a file. Requires `--output-dir` to be set.                                 |
