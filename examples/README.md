# Contoh keluaran

Dibuat dari sumber simulasi (`python -m simulasi --seed 7`), jadi angkanya karangan, bukan cuaca sungguhan. Pipeline dijalankan enam kali dengan simulasi maju satu hari tiap langkah. Simulasi tidak punya data aktual setelah waktu sekarang, jadi jam terbaru tidak melewati saat run dibuat (22 Oktober 2026, 04:00 UTC).

| File | Isi |
|---|---|
| `cuaca_terbaru.csv` | jam aktual terbaru tiap kota |
| `ringkasan_lokasi.csv` | ringkasan per kota |
| `ekstrem_cuaca.csv` | jam hujan lebat atau angin kencang |
| `akurasi_prakiraan.csv` | selisih prakiraan dan aktual per kota |
| `revisi_cuaca.csv` | 20 baris pertama dari 120 angka yang direvisi sumber pada run terakhir |
| `karantina.csv` | satu jam rusak (kelembapan 150%), diambil dari run keempat karena run terakhir tidak punya jam rusak |
| `mart_cuaca_harian.csv` | 24 baris pertama view `mart_cuaca_harian`, hanya hari lengkap |
| `kesehatan.txt` | keluaran `python -m cuaca.kesehatan` setelah run terakhir |
