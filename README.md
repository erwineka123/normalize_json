# Supabase data normalizer

Worker Python ini membaca tabel sumber (default `public.raw_leaks`) dari
project Supabase pertama, menormalkan isi kolom `content`, lalu langsung
memasukkan hasilnya ke tabel tujuan (default `public.leaked_data`) pada project
Supabase kedua. Kedua URL dan key diatur terpisah melalui file `.env`.
Worker memeriksa data baru setiap 60 detik secara default.

## Pemetaan

`content` dapat berupa objek JSON, array objek JSON, atau teks sederhana
dengan pasangan `key:value`. Nama field sumber yang umum (misalnya `nama`,
`no_hp`, `nik_karyawan`, `email`, dan `password`) dipetakan ke kolom tujuan.
Kolom tujuan `id` dibuat otomatis oleh database. Field tujuan lain yang tidak
ditemukan dibiarkan `null`; `file_name`, `source_channel`, dan `Status`
mendapat nilai default dari konfigurasi. `created_at` diambil dari baris
sumber dan `hash-256` dihitung worker.

Konten yang tidak berisi field yang dikenali—misalnya HTML bebas—dilewati
dengan peringatan yang hanya mencantumkan ID baris sumber. Tambahkan alias di
`normalizer/transform.py` bila struktur sumber berbeda.

## Mencegah duplikasi

Worker membuat SHA-256 deterministik dari field data yang dikenali (tanpa
timestamp, status, nama file, dan channel), lalu menyimpannya di kolom
`hash-256`. Unique index memastikan dua worker atau retry tidak memasukkan
record yang sama dua kali. Jalankan SQL berikut **sekali** pada database
tujuan menggunakan Supabase SQL Editor:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS leaked_data_hash_256_uidx
    ON public.leaked_data ("hash-256");

SELECT setval(
    pg_get_serial_sequence('public.leaked_data', 'id'),
    COALESCE((SELECT MAX(id) FROM public.leaked_data), 0) + 1,
    false
);
```

Jika kolom `hash-256` sudah berisi nilai duplikat, bersihkan duplikat tersebut
terlebih dahulu sebelum membuat index. Perintah `setval` menyelaraskan sequence
kolom `id` jika sebelumnya ada baris yang dimasukkan dengan ID manual. Hentikan
worker saat menjalankan SQL tersebut, lalu jalankan kembali worker. Posisi
sumber hanya disimpan di memori. Setelah restart, worker membaca ulang dari
awal; unique hash mencegah data tujuan terduplikasi. Jika tabel sumber besar,
proses membaca ulang ini dapat memerlukan waktu lebih lama. Hindari menjalankan
beberapa worker untuk sumber dan tujuan yang sama secara bersamaan.

## Menjalankan

Prasyarat: Python 3.10+ dan tabel sumber/tujuan sudah tersedia. `.env.example`
berisi konfigurasi terpisah untuk kedua project:

- `SOURCE_SUPABASE_URL` dan `SOURCE_SUPABASE_KEY` untuk membaca database 1.
- `TARGET_SUPABASE_URL` dan `TARGET_SUPABASE_KEY` untuk menulis database 2.
- `SOURCE_TABLE` dan `TARGET_TABLE` untuk memilih nama tabel.

URL Supabase yang diisi harus URL project, misalnya
`https://your-project.supabase.co` (tanpa `/rest/v1`). Worker otomatis
menghapus akhiran `/rest/v1` bila URL lama masih memakai format REST API.

Gunakan key Supabase yang hanya memiliki izin minimum yang dibutuhkan. Key
`service_role` melewati RLS dan tidak boleh dipublikasikan atau dimasukkan ke
Git.

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Isi `.env` dengan URL dan key untuk **kedua** project, lalu jalankan:

```powershell
python -m normalizer
```

Konfigurasi penting lainnya: `POLL_INTERVAL_SECONDS` (default `60`),
`BATCH_SIZE` (default `500`), dan nilai default metadata. Worker melaporkan
kegagalan koneksi/API ke log dan mencoba ulang batch berikutnya tanpa
memajukan posisi baca di memori.

## Tes

```powershell
python -m unittest discover -s tests -v
```
