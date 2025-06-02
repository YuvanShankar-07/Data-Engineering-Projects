# Architecture

This project implements a two-part architecture for detecting transaction patterns from data sourced via Google Drive and processed through AWS S3 and Databricks. It consists of:

- **Mechanism X**: A chunking and uploader engine
- **Mechanism Y**: A detection engine that monitors and processes data with pattern-matching logic

---

## Mechanism X: Transaction Chunk Generator

**Purpose**:  
Runs every second to download a transaction file from Google Drive, split it into 10,000-record chunks, and upload those chunks to an AWS S3 input folder.

### Implementation Details

- **Tooling**:
  - `gdown`: To download the transaction file from Google Drive
  - `boto3`: For AWS S3 interaction
  - Apache Spark (via Databricks): For processing and chunking

### Logic

1. Download the transaction file to a temporary local path using `gdown`
2. Load the CSV into a Spark DataFrame with:
   - Header enabled
   - Quotes removed
3. Add a `uniqueId` column using `row_number()` for chunking separation
4. Count total rows and calculate the number of chunks (10,000 records each) can be processed
5. Loop through and:
   - Filter records by `uniqueId` range
   - Write each chunk to a temporary S3 path
   - Find the `part-*` file and rename it using the pattern:  
     ```
     <filename>__chunk_<chunk_number>.csv
     ```
6. Move the renamed chunk file to the final input folder in S3

-


-----------------------------------------------------------------------------------------

## Mechanism Y: Pattern Detection and Reporting Engine

**Purpose**:  
Monitors S3 input path for new chunk files, processes each file once, applies pattern-matching logic, and outputs detection results in batches of 50 to S3.

###  Implementation Details

- **Database Tables**:
  - `filename_processing`: Prevents duplicate processing
  - `pattern1`, `pattern2`, `pattern3`: Store rules and comparison data

- **Pattern Output Format**:
  Each detection includes:
  - `YStartTime` (IST)
  - `detectionTime` (IST)
  - `patternId`
  - `ActionType`
  - `customerName`
  - `MerchantId`  
  *(Fields not applicable are left as empty strings)*

### Logic

1. Monitor S3 input path on schedule
2. For each new file:
   - Check against `filename_processing` to avoid reprocessing
   - Insert filename into the table to mark as in-progress
   - Create a table for the input filename to capture the detection
3. Load the file into a Spark DataFrame 
4. Apply pattern detection logic using:
   - performing join for `pattern1`, `pattern2`, `pattern3` tables
5. Insert detection records into a dedicated detections table
6. Write detections to S3:
   - Grouped in batches of 50
   - Stored using the naming pattern:
     ```
     <filename>_detection_<chunk_number>.csv
7. Table dropped for the filename
     ```
-----------------------------------------------------------------------------------------------

### Files and Uses :

| Module Name                           | Description                                                                                |
| ------------------------------------- | ------------------------------------------------------------------------------------------ |
| `Customer_importance_Sync_Data`       | Updates pattern detection rules (`pattern1`, `pattern2`, `pattern3`) from a source file.   |

| `Split_as_chunks_with_S3`             | Splits the transaction file into 10,000-record chunks and uploads them to S3.              |

| `Connection_Establishment`            | Establishes a connection to AWS S3 using `boto3`                         |

| `File_Read_from_S3_Input_chunks`      | Reads chunked transaction files from the configured S3 input location.                     |

| `File_Writes_to_S3`                   | Writes detection records from Spark dataframe into S3 as detection output files.      |

| `FolderCreationforTempandDestination` | Creates and manages folder structures for both temporary and final output locations in S3. |

| `Path_Configuration_For_Detection`    | Stores S3 configuration parameters and path definitions used across modules.               |

| `Pattern1_Detection`                  | Detects specific transaction behaviors defined in pattern 1 logic.                         |

| `Pattern2_Detection`                  | Detects specific transaction behaviors defined in pattern 2 logic.                         |

| `Pattern3_Detection`                  | Detects specific transaction behaviors defined in pattern 3 logic.                         |

| `File_Writes_from_table_to_file`	| Retrieves detection records from the filename-specific table and creates a DataFrame for S3 output|


#### OUTPUTS :

Output Files -  Detection Files and Chunk Files saved as Zip

