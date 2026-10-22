# Panduan

Detail yang tidak muat di [README](../README.md).

## Backfill data aktual

Prakiraan tidak punya riwayat, jadi pakai `--mode aktual`:

```bash
python -m cuaca --mode aktual --mulai 2024-01-01 --selesai 2024-12-31
```

Rentang dipecah per 90 hari per kota. Potongan lama disimpan di `data/mentah/`, jadi run ulang tidak memanggil Open-Meteo lagi untuk potongan itu. Satu tahun untuk 12 kota memakai sekitar 60 permintaan.

## Docker dan Airflow

Pipeline di container:

```bash
docker compose --profile app run --rm app
```

Airflow:

```bash
mkdir -p logs output data/mentah   # supaya folder dimiliki akun kamu, bukan root
docker compose --profile airflow up -d
```

Di Linux, isi `AIRFLOW_UID` di `.env` dengan hasil `id -u`. Tanpa itu Airflow bisa gagal menulis ke `logs/`, `output/`, dan `data/`. Setelah mengubah `requirements.txt`, jalankan `docker compose --profile airflow build`.

Buka http://localhost:8080 (login `airflow` / `airflow`), nyalakan DAG `cuaca_harian` (06:00 WIB), lalu Trigger. Task `etl` dan `cek_kesehatan` harus hijau. Tiap run mengambil 7 hari aktual dan 3 hari prakiraan. Rentang tumpang tindih disengaja: data aktual bisa direvisi atau terlambat, dan load idempotent, jadi hari yang terlewat terisi di run berikutnya.

`AIRFLOW_JWT_SECRET` wajib terisi di `.env`. Compose menolak jalan tanpa itu, termasuk untuk `docker compose up -d db`.

## Sumber simulasi

`simulasi/` menyediakan server dengan bentuk respons sama seperti Open-Meteo (`/v1/forecast` dan `/v1/archive`). Angkanya karangan.

- Data aktual tertinggal 5 hari, lalu maju satu hari per langkah.
- Jam yang belum terjadi (setelah waktu server mulai) bernilai null, seperti sumber asli.
- Angka hari aktual terbaru sementara dan sebagian direvisi di langkah berikutnya.
- Tiap empat langkah ada satu jam dengan kelembapan rusak, untuk menguji karantina.
- Prakiraan menyimpang dari kenyataan, makin jauh makin besar.
- Hasil deterministik untuk `--seed` yang sama.

```bash
python -m simulasi --interval 10
python -m cuaca --url-simulasi http://127.0.0.1:8000 --jeda-permintaan 0
```

Jalankan perintah kedua beberapa kali dengan selang lebih dari 10 detik. `--interval 0` membuat simulasi maju hanya lewat `curl -X POST http://127.0.0.1:8000/maju`. Setelah simulasi mengejar hari ini (lima langkah dengan `--hari-awal 5`), tidak ada hari baru. Pasangan untuk `mart_akurasi_prakiraan` baru muncul pada titik itu.

Lewat Compose:

```bash
docker compose --profile simulasi up -d simulasi
```

Lalu isi `OPEN_METEO_FORECAST_URL=http://simulasi:8000/v1/forecast` dan `OPEN_METEO_ARCHIVE_URL=http://simulasi:8000/v1/archive` di `.env`.

## Keluaran

| Lokasi | Isi |
|---|---|
| PostgreSQL `cuaca` | `dim_lokasi`, `dim_tanggal`, `dim_kode_cuaca`, `fact_cuaca_jam`, `fact_prakiraan_jam`, `cuaca_revisi`, `karantina`, view `mart_cuaca_harian` dan `mart_akurasi_prakiraan` |
| `output/cuaca_terbaru.csv` | jam aktual terbaru tiap kota |
| `output/ringkasan_lokasi.csv` | rata-rata, min, maks suhu, total hujan, hari hujan, angin maks per kota, 30 hari terakhir, hanya hari lengkap |
| `output/ekstrem_cuaca.csv` | jam dengan hujan lebat atau angin kencang, 30 hari terakhir |
| `output/akurasi_prakiraan.csv` | selisih rata-rata prakiraan dan aktual per kota dan jarak prakiraan |
| `output/revisi_cuaca.csv` | angka lama yang direvisi sumber pada run ini |
| `output/karantina.csv` | jam yang ditolak pada run ini, dengan alasan |
| `data/mentah/` | cache respons mentah untuk potongan aktual yang sudah final |
| `logs/` | `app.log`, `error.log`, `riwayat_run.jsonl` (satu baris per run) |

Ambang di `ringkasan.py` ditetapkan project ini, bukan standar resmi. Hari hujan: total 1 mm atau lebih. Hujan lebat: 10 mm per jam atau lebih. Angin kencang: 50 km/jam atau lebih.

## Opsi CLI

Daftar lengkap: `python -m cuaca --help`.

| Opsi | Fungsi |
|---|---|
| `--mode semua\|aktual\|prakiraan` | apa yang diambil (bawaan semua) |
| `--hari N` | data aktual N hari terakhir (bawaan 7) |
| `--mulai`, `--selesai` | rentang aktual `YYYY-MM-DD`, menggantikan `--hari` |
| `--hari-prakiraan N` | panjang prakiraan, 1 sampai 16 (bawaan 3) |
| `--lokasi ...` | batasi ke potongan nama kota, pisahkan dengan koma |
| `--jeda-permintaan S` | detik antar permintaan (bawaan 1) |
| `--tanpa-mentah` | jangan pakai cache |
| `--log-stdout` | log ke layar saja |
| `--json` | log format JSON |
| `--url-prakiraan ...`, `--url-arsip ...` | endpoint lain |
| `--url-simulasi ...` | alamat server simulasi, mengatur kedua endpoint |

Daftar kota ada di `LOKASI` di `cuaca/konfigurasi.py`. Tambah kota dengan satu baris di sana.

## Cek kesehatan

`python -m cuaca.kesehatan` membaca `logs/riwayat_run.jsonl`. Kode keluar: 0 OK, 1 WARNING, 2 CRITICAL.

Yang diperiksa:

- run terakhir gagal atau tidak
- umur run sukses terakhir
- rasio jam ditolak
- umur tanggal data aktual terbaru (bawaan 8 hari)
- ada tidaknya prakiraan valid di run yang mengambil prakiraan
- penurunan jumlah jam valid dibanding run sukses sebelumnya dengan mode sama
- ada tidaknya log ERROR

Batas diatur lewat `--maks-umur-jam`, `--maks-rasio`, `--maks-turun`, `--maks-umur-data-hari`. Di Airflow, pemeriksaan ini jalan sebagai task `cek_kesehatan` setelah `etl`.

## Pengembangan

```bash
pip install -r requirements-dev.txt
ruff check .
coverage run -m unittest discover -s tests && coverage report
mypy
```

CI (`.github/workflows/ci.yml`) menjalankan `ruff check`, tes dengan PostgreSQL dan coverage, `mypy` (memblokir), dan validasi `docker-compose.yml`. Di CI, database yang tidak terjangkau membuat tes gagal, bukan di-skip.

Dependensi runtime memakai rentang versi, bukan pin persis, karena `requirements.txt` dipasang ke image Airflow bersama `apache-airflow` dan pin persis mudah bentrok. Untuk instalasi yang bisa direproduksi, buat lockfile: `pip install pip-tools && pip-compile requirements.txt -o requirements.lock`. Batas `fail_under` coverage di `pyproject.toml` adalah angka awal, naikkan setelah melihat hasil CI.

## Struktur

```
cuaca/                kode pipeline
simulasi/             server Open-Meteo simulasi
dags/                 DAG harian Airflow
tests/                tes unittest
docs/                 panduan, kamus data, diagram alur
examples/             contoh keluaran dari sumber simulasi
docker/initdb/        skrip database tes
.github/workflows/    CI
```

## Keamanan

Kredensial bawaan (`cuaca`/`cuaca`, `airflow`/`airflow`) hanya untuk pemakaian lokal. Jangan dipakai di server atau jaringan bersama. Port PostgreSQL, Airflow, dan simulasi hanya dibuka ke `127.0.0.1`. `AIRFLOW_JWT_SECRET` tidak punya nilai bawaan, buat dengan `openssl rand -hex 32`. Hanya `.env.example` yang dikomit.

Kalau mengubah `PGPASSWORD` setelah volume database dibuat, hapus volume (`docker compose down -v`) atau ubah password di PostgreSQL. Image postgres hanya membaca variabel itu saat volume pertama dibuat.

## Catatan teknis

- Respons Open-Meteo berbentuk array paralel. Pipeline memeriksa panjangnya dan berhenti dengan kode keluar 1 kalau bentuknya berubah.
- Nilai `null` berarti tidak ada data. Jam yang semuanya `null` dilewati, dan `null` tidak menimpa nilai tersimpan.
- Kunci data aktual: (kota, `waktu_utc`). Angka yang direvisi memperbarui fakta dan tercatat di `cuaca_revisi`.
- Potongan aktual yang berakhir lebih dari 90 hari lalu dianggap final dan di-cache. Angka 90 adalah pilihan project ini, ubah lewat `HARI_FINAL` di `extract.py`. Prakiraan tidak pernah di-cache.
- Prakiraan yang jamnya sudah lewat saat diambil dibuang. Sebaliknya, jam aktual yang belum terjadi juga dibuang, karena archive bisa mengisinya dengan estimasi model.
- `waktu_prediksi` adalah waktu pipeline mengambil data (dibulatkan ke jam), bukan waktu model dijalankan, karena sumber tidak memberikannya.
- Run kedua dalam jam yang sama tidak menambah potret prakiraan. Potret pertama dipertahankan.
- Perbandingan prakiraan dan aktual memakai data reanalisis. Pasangan baru muncul setelah jam target lewat dan data aktualnya masuk.
- Tabel `karantina` bertambah tiap run, termasuk untuk jam rusak yang sama.
- CSV memakai koma. Excel berbahasa Indonesia sering membacanya jadi satu kolom, pakai Data, From Text/CSV.
- Port bawaan (5432, 8000, 8080) bisa bentrok dengan project lain. Hentikan yang satu atau ubah `PGPORT` di `.env`.
