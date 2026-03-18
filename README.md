# CTA


### Installation

---
1. Clone the repository

   - Using SSH
   ```shell script
   git clone xxx
   ``` 
   
   - Using HTTPS with Personal Access Token
   ```shell script
   git clone xxx
   ```

2. Set up the Virtual Environment

    Ubuntu 20.04 (Debian-based Linux)
    ```shell script
    cd ./get_bybit_ohlcv
    python3.12 -m venv venv/
    source ./venv/bin/activate
    ```
   
    Windows 10
    ```shell script
    cd .\get_bybit_ohlcv
    python -m venv .\venv\
    .\venv\Scripts\activate
    ```

3. Install the dependencies

    ```shell script
    pip install -r requirements.txt
    pip install --upgrade pip
    ```


### Deployment

---
#### Dev Environment
1. Run the application
    ```shell script
    python3.12 main_parquet.py
    ```

#### Running via Systemd
1. Move the file to Systemd's system folder.
    ```shell script
    sudo cp ./get_bybit_ohlcv.service /etc/systemd/system/get_bybit_ohlcv.service
    ```
2. Enable and start the service.
    ```shell script
    sudo systemctl daemon-reload
    sudo systemctl enable get_bybit_ohlcv.service
    sudo systemctl start get_bybit_ohlcv.service
    ```
3. Check if the application is running.
    ```shell script
    sudo systemctl status get_bybit_ohlcv.service
    ```
# get_bybit_ohlcv
# get_bybit_ohlcv
