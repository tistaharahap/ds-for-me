# -*- coding: utf-8 -*-
from text.classifiers import NaiveBayesClassifier
from textblob import TextBlob
import feedparser
import time
import redis
import hashlib
import json


TIMEOUT = 60*60

REDIS_HOST = '10.130.254.85'
REDIS_PORT = 6379


def feature_extractor(text):
    if not isinstance(text, TextBlob):
        text = TextBlob(text.lower())

    return {
        'has_rumor': 'rumor' in text.words,
        'has_gosip': 'gosip' in text.words,
        'has_urbanesia': 'urbanesia' in text.words,
        'has_batista': 'batista' in text.words,
        'has_harahap': 'harahap' in text.words,
        'has_pemasaran': 'pemasaran' in text.words,
        'has_saham': 'saham' in text.words,
        'has_hackathon': 'hackathon' in text.words,
        'has_ipo': 'ipo' in text.words,
        'has_akuisisi': 'akuisisi' in text.words,
        'has_startup': 'startup' in text.words,
        'has_android': 'android' in text.words,
        'has_aplikasi': 'aplikasi' in text.words,
        'has_payment': 'payment' in text.words,
        'has_pembayaran': 'pembayaran' in text.words,
        'has_api': 'api' in text.words,
        'has_kompetisi': 'kompetisi' in text.words,
        'has_ide': 'ide' in text.words,
        'has_permainan': 'permainan' in text.words,
        'has_game': 'game' in text.words,
        'has_fundraising': 'fundraising' in text.words,
        'has_askds': '[Ask@DailySocial]' in text.words,
        'has_investasi': 'investasi' in text.words,
        'has_musik': 'musik' in text.words,
        'has_lagu': 'lagu' in text.words,
        'has_bhinneka': 'bhinneka' in text.words,
        'has_marketplace': 'marketplace' in text.words,
        'has_mobile': 'mobile' in text.words,
        'has_cto': 'cto' in text.words,
        'has_traffic': 'traffic' in text.words,
        'starts_with_[': text[0] == '['
    }

train_set = [
    ('Berbarengan dengan Launch Festival, Ice House Buka Kompetisi Wujudkan Ide-Ide Aplikasi Mobile.', 'ok'),
    ('Ulang Tahun Ke-21, Layanan E-Commerce Bhinneka Segera Perbarui Platform E-Commerce dan Luncurkan Marketplace Terkurasi.', 'ko'),
    ('Aplikasi Pencatat Blastnote Hadir di Android.', 'ok'),
    ('Portal Hiburan Digital UZone Kini Hadir Dalam Versi Aplikasi Mobile.', 'ok'),
    ('CTI IT Infrastructure Summit 2014 Bahas Big Data Sebagai Tren Teknologi', 'ko'),
    ('Dua Berita Buruk Besar Bagi Blackberry', 'ok'),
    ('Tanggapan Pelaku Industri Digital di Indonesia tentang Fenomena Permainan Mobile Flappy Bird', 'ok'),
    ('[Ask@DailySocial] Proses Fundraising Untuk Startup', 'ok'),
    ('Investasi $1 Miliar, Foxconn Pastikan Bangun Pabriknya di DKI Jakarta', 'ok'),
    ('Raksasa Digital Cina Tencent Dikabarkan Akuisisi Portal Berita Okezone', 'ko'),
    ('Wego Tawarkan Akses Reservasi Tiket dan Hotel Lebih Mudah Melalui Aplikasi Mobile', 'ok'),
    ('Telkom Hadirkan Agen Wisata Online Hi Indonesia', 'ko'),
    ('Meski Didera Isu Fake Likes, Facebook Tetap Jadi Pilihan Utama Untuk Pemasaran Digital', 'ok'),
    ('Dave Morin Pastikan Saham Bakrie Global Group di Path Kurang dari 1%', 'ok'),
    ('Kecil Kemungkinan Pemerintah Tutup Telkomsel dan Indosat Terkait Dugaan Penyadapan oleh Australia', 'ok'),
    ('Kakao Dikabarkan Gelar Penawaran Saham Perdana Tahun Depan', 'ok'),
    ('Ericsson Akan Hadirkan Layanan Streaming TV', 'ok'),
    ('Ryu Kawano: Ingin Startup Anda Go Global? Tunggu Dulu!', 'ok'),
    ('Kerja Sama dengan GHL Systems Malaysia, Peruri Digital Security Kembangkan Sistem Pembayaran Online', 'ok'),
    ('Aplikasi Logbook Travel Kini Telah Hadir di Android', 'ok'),
    ('Musikator Hadirkan Layanan Agregator Lagu Untuk Distribusi Digital', 'ok'),
    ('[Manic Monday] Strategi Produksi Konten Di Era Multilayar', 'ok'),
    ('Bakrie Telecom Jajaki Kemungkinan Carrier Billing untuk Path', 'ok'),
    ('Viber Secara Resmi Telah Diakuisisi Oleh Rakuten Sebesar US$ 900 Juta', 'ok'),
    ('Situs Panduan Angkutan Umum Kiri.travel Buka API, Tantang Pengembang Buat Aplikasi Windows Phone', 'ok'),
    ('Wego Luncurkan Jaringan Afiliasi WAN.Travel', 'ko'),
    ('Business Insider Masuki Pasar Indonesia Bekerja Sama dengan REV Asia', 'ko'),
    ('Waze Memiliki 750.000 Pengguna di Indonesia', 'ok'),
    ('Survei Nielsen: Masyarakat Asia Tenggara Lebih Suka Gunakan Uang Tunai untuk Belanja Online', 'ok'),
    ('CTI IT Infrastructure Summit 2014 Bahas Big Data Sebagai Tren Teknologi', 'ko'),
    ('Pacu Bisnis di Asia Tenggara, Game Online Asing Kini Lebih Lokal', 'ko'),
    ('Enam Pilihan Layanan Streaming Musik Yang Dapat Dinikmati di Indonesia', 'ok'),
    ('Country Manager Yahoo Indonesia Roy Simangunsong Mengundurkan Diri', 'ko'),
    ('Investasi $1 Miliar, Foxconn Pastikan Bangun Pabriknya di DKI Jakarta', 'ok'),
    ('Jomblo.com Tawarkan Media Sosial Untuk Mencari Jodoh', 'ko'),
    ('Mitra Adiperkasa dan Groupon Pilih aCommerce Indonesia untuk Pusat Logistik dan Pengiriman Layanan E-Commerce', 'ko'),
    ('Transformasi Portal Informasi Kecantikan Female Daily Disambut Positif, Beberkan Rencana-Rencana 2014', 'ko'),
    ('Visa Gelar Promosi Diskon Setiap Jumat Bekerja Sama dengan Enam Layanan E-Commerce Lokal', 'ko'),
    ('Kerjasama Strategis, Blue Bird Group Benamkan Teknologi Interkoneksi Microsoft Ke Armada Premium Big Bird', 'ko'),
    ('Ramaikan Industri Fashion E-Commerce Indonesia, VIP Plaza Hadir Tawarkan Promo Flash Sale', 'ko'),
    ('Bidik Citizen Journalism, Detik Hadirkan Media Warga PasangMata', 'ko'),
    ('Asia Pasifik Jadi Kawasan E-Commerce B2C Terbesar di Dunia Tahun 2014', 'ko'),
    ('CTO Urbanesia Batista Harahap Mengundurkan Diri', 'ok'),
    ('Tees Indonesia Alami Peningkatan Traffic Hingga 7x, Namun Tidak Seperti Yang Anda Kira', 'ok')
]

cl = NaiveBayesClassifier(train_set=train_set, 
                          feature_extractor=feature_extractor)

redis_conn = redis.StrictRedis(host=REDIS_HOST, 
                               port=REDIS_PORT)


def get_feed():
    feed_url = 'http://feeds.feedburner.com/dsnet?format=xml'
    feeds = feedparser.parse(feed_url).get('entries')

    if feeds is None:
        return

    def process_entry(entry):
        def process_tags(tags):
            return [tag.get('term') for tag in tags]

        cls = cl.classify(text=entry.get('title'))

        data = {
            'author': entry.get('author'),
            'title': entry.get('title'),
            'link': entry.get('link'),
            'published': int(time.mktime(entry.get('published_parsed'))),
            'summary': entry.get('summary'),
            'tags': process_tags(entry.get('tags')),
            'class': cls
        }

        return data if cls == 'ok' else None

    feeds = [process_entry(entry) for entry in feeds]

    return [entry for entry in feeds if entry is not None]

def md5(text):
    m = hashlib.md5()
    m.update(text.encode('utf-8'))
    return m.hexdigest()

def cycle():
    try:
        posts = get_feed()
    except KeyError:
        print 'Unreadable RSS feed, bailing..'
        return

    if not posts:
        print 'Got nothing, bailing..'
        return

    def redis_insert(post):
        name = 'ds-articles-ok'

        redis_conn.zadd(name, post.get('published'), json.dumps(post))

    [redis_insert(post=post) for post in posts]

    print 'Got %d posts this time.' % len(posts)


if __name__ == '__main__':
    print 'Starting up..'
    while True:
        cycle()
        print 'Sleeping for %s seconds.' % TIMEOUT
        time.sleep(TIMEOUT)
