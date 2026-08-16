# 🗼 Babel Gateway: Async API Middleware

Babel Gateway adalah sebuah layanan *middleware* berkinerja tinggi yang bertindak sebagai *Single Point of Contact* (SPoC) untuk menjembatani klien HTTP/REST dengan berbagai layanan *backend* (Service A, Service B, Service C) yang berjalan di atas ragam protokol tingkat rendah yang berbeda. 

Sistem ini direkayasa secara murni *non-blocking* menggunakan FastAPI dan Python `asyncio`, dirancang dengan prinsip-prinsip *cloud networking* dan orkestrasi infrastruktur yang kokoh untuk menjamin *failure isolation* dan konkurensi lalu lintas data tingkat tinggi.

## 👨‍💻 Penulis
* **Nama:** Muchammad Rafif Azis Syahlevi
* **NIM:** 18224123
* **Program Studi:** Sistem dan Teknologi Informasi (STI)
* **Institusi:** Institut Teknologi Bandung (ITB)

## ✨ Fitur Utama & Pemenuhan Kriteria
Gateway ini telah divalidasi berhasil memenuhi 9 komponen pengujian rekayasa *middleware*:
1. **Startup Gateway:** Inisialisasi *event-loop* Uvicorn dan *Service Registry* secara instan.
2. **Koneksi Backend:** Orkestrasi jaringan internal (*bridge*) menggunakan Docker Compose tanpa mengekspos *port* publik pada *backend*.
3. **Translasi Protokol Level Rendah:**
   * **Service A (HTTP/JSON):** *Asynchronous reverse proxy*.
   * **Service B (TCP):** Pembuatan *custom binary framing* 16-*byte* *Big-Endian* untuk batas *streaming* TCP menggunakan *Magic Word* `0xBABE`.
   * **Service C (UDP):** Manipulasi *datagram* biner 24-*byte* yang dilengkapi kalkulasi dan verifikasi *checksum* `zlib.crc32`.
4. **Routing Berbasis Capability:** Pemetaan dinamis ke *backend* berdasarkan operasi logis dan dukungan infrastruktur *fallback*.
5. **Permintaan Konkuren:** Manajemen ID *request* yang *thread-safe* untuk melayani ribuan koneksi bersamaan tanpa insiden *race condition*.
6. **Penanganan Timeout:** Pembatasan waktu tunggu mutlak (*Fail-Fast*) untuk melindungi *Gateway* dari kondisi *hang* akibat *backend* yang lambat.
7. **Penolakan Response Rusak:** Validasi integritas paket CRC-32 secara mandiri pada layanan UDP (Service C).
8. **Isolasi Kehancuran (*Failure Isolation*):** Menolak operasi yang tidak valid dan menahan *crash* dari satu *backend* agar tidak menjatuhkan operasional keseluruhan sistem.
9. **Ketahanan Konfigurasi:** Skema *registry In-Memory* yang langsung pulih seutuhnya pasca siklus *restart* layanan.

## 📂 Struktur Direktori (*Deliverables*)
Seluruh komponen kode dan dokumen pendukung (*deliverables*) telah disatukan di dalam direktori utama ini:

```text
tower-of-babel/
├── src/
│   └── gateway/
│       ├── main.py                 # Source code utama API Gateway
│       ├── requirements.txt        # Dependensi Python
│       └── Dockerfile              # Instruksi build image Gateway
├── demo/
│   └── Skrip_Demonstrasi.pdf       # Skenario 6 adegan untuk video demonstrasi
├── docker-compose.yml              # Orkestrasi jaringan dan container (Gateway + Backend)
├── submission.json                 # Metadata pengumpulan tugas
├── Laporan.pdf                     # Laporan rekayasa arsitektur teknis Gateway
├── DEKLARASI_AI.pdf                # Deklarasi integritas akademik penggunaan AI
└── README.md                       # Dokumentasi proyek ini