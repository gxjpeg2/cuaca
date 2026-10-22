# Kamus data

Untuk analis yang memakai database `cuaca`. Gambaran model dan keputusan desainnya ada di [README](../README.md#model-data).

Aturan umum:

- Semua waktu dengan akhiran `_utc` atau `waktu_*` bertipe `TIMESTAMPTZ` dan nilainya UTC.
- `tanggal_lokal` dan `jam_lokal` memakai zona waktu kota (`dim_lokasi.zona_waktu`): WIB (UTC+7), WITA (UTC+8), atau WIT (UTC+9). Indonesia tidak memakai waktu musim panas.
- Nilai kosong disimpan sebagai `NULL`, tidak pernah sebagai 0.
- Data aktual berasal dari reanalisis (rekonstruksi cuaca berbasis model), bukan pengukuran stasiun.

## Kolom ukuran

Sama di `fact_cuaca_jam` dan `fact_prakiraan_jam`.

| Kolom | Tipe | Satuan | Boleh dijumlahkan | Arti |
|---|---|---|---|---|
| `suhu_c` | NUMERIC(4,1) | derajat Celsius | tidak (pakai AVG, MIN, MAX) | suhu udara 2 meter di atas tanah |
| `kelembapan_persen` | SMALLINT | persen, 0 sampai 100 | tidak | kelembapan relatif 2 meter di atas tanah |
| `curah_hujan_mm` | NUMERIC(6,1) | milimeter per jam | ya (SUM) | curah hujan dalam jam tersebut |
| `kecepatan_angin_kmh` | NUMERIC(5,1) | kilometer per jam | tidak | kecepatan angin 10 meter di atas tanah |
| `tekanan_hpa` | NUMERIC(6,1) | hektopascal | tidak | tekanan udara yang disesuaikan ke permukaan laut |
| `kode_cuaca` | SMALLINT | kode WMO | tidak | kondisi cuaca, lihat `dim_kode_cuaca` |

Rentang kewajaran yang diperiksa pipeline (di luar rentang masuk karantina): suhu -90 sampai 60, kelembapan 0 sampai 100, hujan 0 sampai 500 mm per jam, angin 0 sampai 400 km/jam, tekanan 850 sampai 1100 hPa. Ini batas kasar untuk membuang data rusak, bukan batas fisik.

## fact_cuaca_jam

Grain: 1 baris = 1 kota pada 1 jam. Data aktual.

| Kolom | Tipe | Arti |
|---|---|---|
| `lokasi_id` | INTEGER | kunci ke `dim_lokasi` |
| `waktu_utc` | TIMESTAMPTZ | awal jam yang dimaksud, UTC. Bersama `lokasi_id` menjadi kunci utama |
| `tanggal_lokal` | DATE | tanggal di zona waktu kota, kunci ke `dim_tanggal` |
| `jam_lokal` | SMALLINT | jam di zona waktu kota, 0 sampai 23 |
| `kode_cuaca` | SMALLINT | kunci ke `dim_kode_cuaca`, bisa NULL |
| kolom ukuran | | lihat tabel di atas |
| `diperbarui_run` | TEXT | id run pipeline yang terakhir menulis baris ini |

Catatan: baris untuk jam yang belum tersedia di sumber tidak ada. Angka bisa direvisi sumber, riwayat revisinya ada di `cuaca_revisi`.

## fact_prakiraan_jam

Grain: 1 baris = 1 kota pada 1 jam target menurut 1 potret prakiraan.

| Kolom | Tipe | Arti |
|---|---|---|
| `lokasi_id` | INTEGER | kunci ke `dim_lokasi` |
| `waktu_target` | TIMESTAMPTZ | jam yang diprakirakan, UTC |
| `waktu_prediksi` | TIMESTAMPTZ | waktu pipeline mengambil prakiraan, dibulatkan ke jam. Bukan waktu model cuaca dijalankan |
| `jarak_jam` | SMALLINT | `waktu_target` dikurangi `waktu_prediksi`, dalam jam. Selalu 0 atau lebih |
| `kode_cuaca` | SMALLINT | kunci ke `dim_kode_cuaca`, bisa NULL |
| kolom ukuran | | lihat tabel di atas |
| `run_id` | TEXT | id run pipeline yang menulis baris ini |

Satu jam target punya banyak baris, satu per potret. Untuk memakai prakiraan terbaru saja, ambil baris dengan `waktu_prediksi` terbesar per (`lokasi_id`, `waktu_target`).

## dim_lokasi

| Kolom | Tipe | Arti |
|---|---|---|
| `lokasi_id` | INTEGER | kunci pengganti |
| `nama` | TEXT | nama kota, unik |
| `provinsi` | TEXT | provinsi |
| `lintang`, `bujur` | NUMERIC(8,5) | koordinat yang dikirim ke sumber |
| `elevasi_m` | NUMERIC(7,1) | ketinggian titik grid menurut sumber, dalam meter. Bukan ketinggian pusat kota |
| `zona_waktu` | TEXT | `Asia/Jakarta`, `Asia/Makassar`, atau `Asia/Jayapura` |

## dim_tanggal

Kalender 2000-01-01 sampai 2039-12-31, dibuat sekali.

| Kolom | Arti |
|---|---|
| `tanggal` | kunci |
| `tahun`, `kuartal`, `bulan` | angka, kuartal 1 sampai 4 |
| `nama_bulan`, `nama_hari` | dalam bahasa Indonesia |
| `hari_dalam_minggu` | 1 = Senin sampai 7 = Minggu (ISO) |
| `akhir_pekan` | true untuk Sabtu dan Minggu |

## dim_kode_cuaca

| Kolom | Arti |
|---|---|
| `kode_cuaca` | kode WMO yang dipakai Open-Meteo |
| `deskripsi` | penjelasan singkat, bahasa Indonesia |
| `kategori` | cerah, berawan, kabut, gerimis, hujan, salju, atau badai |

## mart_cuaca_harian (view)

Grain: 1 kota per tanggal lokal.

| Kolom | Arti |
|---|---|
| `lokasi_id`, `lokasi`, `provinsi` | identitas kota |
| `tanggal` | tanggal lokal |
| `jumlah_jam` | jumlah jam yang punya data pada hari itu |
| `hari_lengkap` | true kalau `jumlah_jam` = 24. Filter dengan ini untuk rata-rata dan total yang adil |
| `suhu_min_c`, `suhu_maks_c`, `suhu_rata_c` | ringkasan suhu |
| `kelembapan_rata_persen` | rata-rata kelembapan |
| `curah_hujan_total_mm` | jumlah curah hujan sehari |
| `jam_hujan` | jumlah jam dengan curah hujan 0,1 mm atau lebih |
| `angin_maks_kmh` | kecepatan angin per jam tertinggi |

## mart_akurasi_prakiraan (view)

Grain: 1 kota per kelompok jarak prakiraan. Hanya memuat jam yang punya prakiraan sekaligus data aktual.

| Kolom | Arti |
|---|---|
| `lokasi` | nama kota |
| `kelompok_jarak` | `0-23 jam`, `24-47 jam`, atau `48 jam ke atas` sebelum jam target |
| `jumlah_pasangan` | jumlah pasangan prakiraan dan data aktual |
| `mae_suhu_c` | rata-rata selisih mutlak suhu, derajat Celsius |
| `bias_suhu_c` | rata-rata selisih bertanda (prakiraan dikurangi aktual). Positif berarti prakiraan cenderung lebih panas |
| `mae_hujan_mm` | rata-rata selisih mutlak curah hujan per jam |

Data aktual baru dipakai setelah jam targetnya lewat, jadi view ini kosong atau sedikit isinya untuk prakiraan yang masih baru.

## cuaca_revisi

Satu baris per kolom yang angkanya diganti sumber pada data aktual yang sudah tersimpan.

| Kolom | Arti |
|---|---|
| `run_id` | run pipeline yang mendeteksi revisi |
| `lokasi_id`, `waktu_utc` | jam yang direvisi |
| `kolom` | nama kolom ukuran yang berubah |
| `nilai_lama`, `nilai_baru` | angka sebelum dan sesudah |

## karantina

Jam yang ditolak pipeline.

| Kolom | Arti |
|---|---|
| `run_id` | run pipeline yang menolak |
| `jenis` | `aktual` atau `prakiraan` |
| `lokasi`, `waktu_utc` | nama kota dan waktu mentah dari sumber (teks) |
| `alasan` | kolom mana yang rusak dan kenapa |
| `nilai_mentah` | isi jam itu dari sumber, apa adanya |
| `dicatat_pada` | waktu pencatatan |
