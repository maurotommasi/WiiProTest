# Environment Setup Instructions (Windows)

To set up the environment for running the provided Python code, follow these steps:

1. Open a PowerShell (PS) terminal.

2. Create a virtual environment using the following commands:

   ```powershell
   PS> python -m venv .venv
    ```

    ```powershell
   PS> .\.venv\Scripts\Activate.ps1
    ```

    ```powershell
    PS> pip install pandas
    ```

3. Refer to wiipro.ipynb file to read the sample case

4. Data, for this example, has to be in a csv format, transformation between txt and csv is not included in the code provided.

5. Before running the wiipro class engine, it's necessary to generate some random data to understand better the performance of the code developed. It's suggested to open the generate_random_data.ipynb and run the cell-code.

6. Please consider that the code provided is only for demonstration purpose to understand the performance of the code requested. Any other implementation (such DB connection, REST API call, python executable code) are not implemented.