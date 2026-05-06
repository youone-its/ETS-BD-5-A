# WeatherPulse - Big Data Weather Pipeline

WeatherPulse is a real-time data pipeline designed to ingest, process, and store weather information from multiple sources. This project leverages **Apache Kafka** for reliable messaging and **Apache Hadoop (HDFS)** for persistent storage of large-scale weather datasets.

## Architecture

```mermaid
graph TD
    A1[Open-Meteo API] -->|Fetch| P[Producer - main.ipynb]
    A2[RSS Feeds - Tempo/Kompas] -->|Fetch| P
    P -->|Publish| K[Kafka Broker]
    K -->|topic: weather-api| C[Consumer - main.ipynb]
    K -->|topic: weather-rss| C
    C -->|Store| H[HDFS - Namenode / Data/weather]
```

## Features

- **Real-time API Ingestion**: Monitors weather conditions (temperature, humidity, wind speed) for 6 major Indonesian cities (Jakarta, Surabaya, Semarang, Medan, Makassar, Denpasar).
- **RSS Feed Integration**: Scrapes weather-related news from national news outlets.
- **Message Queuing**: Uses Kafka to decouple data ingestion from storage logic.
- **Distributed Storage**: Automatically flushes ingested data into Hadoop HDFS for future analysis.
- **Dockerized Infrastructure**: Complete setup using Docker Compose for easy deployment.

##  Repository Structure

- `hadoop/`: Docker configuration and setup scripts for the Hadoop cluster (Namenode, Datanode, ResourceManager, NodeManager).
- `kafka/`: Docker configuration for the Kafka broker and controller.
- `main.ipynb`: Core Python logic containing producers, consumers, and data formatting.

##  Prerequisites

- Docker & Docker Compose
- Python 3.x
- Jupyter Notebook
- Python libraries: `kafka-python`, `requests`, `feedparser`

##  Getting Started

### 1. Launch Infrastructure

Ensure your Docker daemon is running, then start the services:

```bash
# Start Hadoop cluster
cd hadoop
docker-compose up -d

# Start Kafka broker
cd ../kafka
docker-compose up -d
```

### 2. Configuration & Initialization

Run the setup scripts to initialize Kafka topics and HDFS directories. 
> [!NOTE]
> There is a slight folder naming discrepancy in the setup scripts; follow these exact commands:

```bash
# Initialize Kafka topics (located in hadoop/ folder)
cd ../hadoop
bash hadoop_setup.sh

# Initialize HDFS directories (located in kafka/ folder)
cd ../kafka
bash kafka_setup.sh
```

### 3. Run the Pipeline

Open `main.ipynb` in your Jupyter environment and run the cells sequentially:
1.  **Section 1 (Data Source)**: Connects to APIs and RSS feeds.
2.  **Section 2 (Data Ingest)**: Starts threads to push data into Kafka.
3.  **Section 3 (Data Store)**: Starts consumers to save data into HDFS.

##  Data Locations

- **HDFS API Data**: `/data/weather/api/`
- **HDFS RSS Data**: `/data/weather/rss/`

# 3. Producer RSS & Consumer to HDFS

Membuat:

### 1. Producer RSS (producer_rss.py)
Dimana producer ini bertugas mengambil data berita dari beberapa sumber RSS, kemudian mengirimkannya ke Kafka topic `weather-rss`.

Fitur utama:
- Mengambil data dari:
  - Antara News (Warta Bumi)
  - Mongabay Indonesia
- Melakukan deduplikasi artikel agar tidak dikirim berulang
- Menggunakan hashing URL sebagai key Kafka
- Mengirim data setiap 5 menit (300 detik)

Struktur data yang dikirim adalah:
- judul
- link
- ringkasan
- waktu terbit
- sumber
- timestamp


### 2. Consumer to HDFS (consumer_to_hdfs.py)
Consumer ini bertugas membaca data dari Kafka dan menyimpannya ke dalam HDFS.

Fitur utama:
- Mengonsumsi data dari 2 topic:
  - weather-api
  - weather-rss
- Menggunakan threading untuk memproses data API dan RSS secara paralel
- Menyimpan data sementara di buffer
- Melakukan flush ke HDFS setiap 2 menit
- Menggunakan Docker untuk mengakses container Hadoop

Alur penyimpanan:
1. Data dikumpulkan di buffer
2. Disimpan sementara sebagai file JSON lokal
3. Dipindahkan ke container Hadoop
4. Disimpan ke HDFS

Path penyimpanan:
- API  → /data/weather/api
- RSS  → /data/weather/rss

## Cara Menjalankan (Producer RSS & Consumer HDFS)

Setelah kafka dan Hadoop (Docker) berjalan lalu

### 1. Menjalankan Producer RSS
```python producer_rss.py```

Producer akan:
- Mengambil data RSS setiap 5 menit
- Mengirim artikel ke Kafka topic `weather-rss`

### 2. Menjalankan Consumer ke HDFS
Diterminal lain:
```python consumer_to_hdfs.py```

Consumer akan:
- Membaca data dari Kafka
- Menyimpan data ke buffer
- Mengirim data ke HDFS setiap 2 menit


### 3. Hasil
- Data berita masuk ke Kafka
- Data tersimpan di HDFS dalam bentuk file JSON
- File tersimpan berdasarkan timestamp

#### Producer RSS Berjalan

- Menampilkan proses polling RSS
- Menampilkan jumlah artikel yang dikirim

#### Consumer to HDFS Berjalan
(Tampilkan screenshot terminal saat consumer_to_hdfs.py berjalan)
- Menampilkan proses konsumsi data
- Menampilkan status berhasil/gagal penyimpanan ke HDFS

#### Data di HDFS
(Tampilkan screenshot hasil di HDFS, misalnya menggunakan command:)
hdfs dfs -ls /data/weather/rss

- Menunjukkan file JSON berhasil tersimpan

# 5. DASHBOARD

Dashboard ini bertujuan untuk membangun sistem data pipeline yang menggabungkan:
- Data streaming (real-time) dari Kafka
- Data batch hasil analisis Spark
- Dashboard sederhana menggunakan Flask

Output akhir berupa dashboard web yang menampilkan data historis dan data live.

### Arsitektur Sistem

Sistem terdiri dari beberapa komponen utama:

1. Producer → Mengirim data ke Kafka
2. Kafka → Message broker
3. Consumer → Mengambil data dan menyimpan ke JSON
4. Spark → Analisis data historis
5. Flask Dashboard → Menampilkan data ke user

### Cara Menjalankan Sistem

Untuk menjalankan sistem ini secara keseluruhan, diperlukan beberapa terminal yang berjalan secara bersamaan.

#### 1. Menjalankan Producer API (Data Cuaca)
Buka terminal pertama dan jalankan:
```python producer_api.py```

Producer ini akan mengirim data cuaca ke Kafka secara real-time.

#### 2. Menjalankan Producer RSS (Data Berita)
Buka terminal kedua dan jalankan:
```python producer_rss.py```

Producer ini akan mengirim data berita terbaru ke Kafka.

#### 3. Menjalankan Dashboard (Flask)
Buka terminal ketiga dan jalankan:
```python app.py```

Kemudian buka browser dan akses:
http://localhost:5000

Dashboard akan menampilkan data historis dan data live.

#### Hasil Akhir
Setelah semua komponen berjalan:
- Dashboard akan menampilkan data dari Spark (historis)
- Data live akan terus diperbarui dari Kafka
- Halaman akan auto-refresh setiap 30 detik

<img width="1600" height="999" alt="WhatsApp Image 2026-05-07 at 00 16 41" src="https://github.com/user-attachments/assets/8ed83af3-42c4-45a3-9680-5e33d92807c5" />

<img width="1600" height="999" alt="WhatsApp Image 2026-05-07 at 00 16 41 (1)" src="https://github.com/user-attachments/assets/61c773ec-fedf-4060-bd41-f143329b1f5e" />

<img width="1600" height="999" alt="WhatsApp Image 2026-05-07 at 00 16 42" src="https://github.com/user-attachments/assets/34856749-9491-4f63-adc4-ecbf0cef7a93" />

<img width="1600" height="999" alt="WhatsApp Image 2026-05-07 at 00 16 43" src="https://github.com/user-attachments/assets/cee2292c-77c7-4496-b22c-34389541a541" />

---
*Created as part of the Big Data course (ETS-BD-5-A)*
