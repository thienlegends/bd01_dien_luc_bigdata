async function taiTinhTrangPipeline() {
  const el = document.getElementById('tinh-trang-pipeline');
  const mode = document.getElementById('che-do-du-lieu');
  try {
    const s = await getJSON('/api/tinh-trang');
    const demo = s.che_do_du_lieu === 'demo';
    mode.className = 'status-pill ' + (demo ? 'status-demo' : (s.che_do_du_lieu === 'du_lieu_that' ? 'status-ok' : 'status-warn'));
    mode.textContent = demo ? 'ĐANG DÙNG DỮ LIỆU DEMO' : (s.che_do_du_lieu === 'du_lieu_that' ? 'CACHE DỮ LIỆU' : 'CHƯA CÓ CACHE');
    const names = {
      phu_tai_15p: 'Phụ tải thực tế', du_bao_phu_tai: 'Dự báo phụ tải',
      canh_bao_bat_thuong: 'Cảnh báo bất thường', tieu_thu: 'Tiêu thụ khách hàng', du_lieu_tre: 'Dữ liệu đến trễ'
    };
    const cards = Object.entries(s.bang_cache).map(([k, v]) =>
      '<div class="status-item"><span>' + esc(names[k] || k) + '</span><b class="' + (v.san_sang ? 'txt-ok' : 'txt-warn') + '">' +
      (v.san_sang ? 'Sẵn sàng' : 'Thiếu dữ liệu') + '</b><small>' + v.so_file + ' file Parquet</small></div>').join('');
    const airflow = '<div class="status-item"><span>Airflow REST API</span><b class="' + (s.airflow.ket_noi_duoc ? 'txt-ok' : 'txt-warn') + '">' +
      (s.airflow.ket_noi_duoc ? 'Đã kết nối' : 'Chưa kết nối') + '</b><small>' + esc(s.airflow.url) + '</small></div>';
    const delta = '<div class="status-item"><span>Ghi kết luận vào Delta</span><b class="' + (s.ghi_delta_bat ? 'txt-ok' : 'txt-warn') + '">' +
      (s.ghi_delta_bat ? 'Đã bật' : 'Đang tắt') + '</b><small>Đồng bộ nền qua spark-submit</small></div>';
    el.innerHTML = cards + airflow + delta;
  } catch (e) {
    mode.textContent = 'KHÔNG KIỂM TRA ĐƯỢC';
    el.innerHTML = '<div class="ghi-chu">' + esc(e.message) + '</div>';
  }
}

(async function () {
  taiTinhTrangPipeline();
  document.getElementById('btn-lam-moi-trang-thai').addEventListener('click', taiTinhTrangPipeline);
  try {
    const [pt, db, kpi] = await Promise.all([
      getJSON('/api/phu-tai'), getJSON('/api/du-bao'), getJSON('/api/kpi'),
    ]);

    document.getElementById('kpi-mape').textContent = fmtPct(kpi.mape_7ngay);
    document.getElementById('kpi-mape-sub').textContent =
      '1–4: ' + fmtPct(kpi.mape_1_4) + ' · 5–24: ' + fmtPct(kpi.mape_5_24) + ' · 25–96: ' + fmtPct(kpi.mape_25_96);
    document.getElementById('kpi-mat').textContent = kpi.so_khach_hang_mat_tin_hieu;
    document.getElementById('kpi-moi').textContent = kpi.so_canh_bao_moi;
    document.getElementById('kpi-moc').textContent = fmtT(kpi.moc_du_lieu);
    document.getElementById('kpi-mohinh').textContent =
      db.phien_ban_mo_hinh ? 'Mô hình: ' + db.phien_ban_mo_hinh : 'Chưa có dự báo';

    const act = pt.diem, fc = db.diem;
    const na = act.length, nf = fc.length;
    const labels = act.map((x) => x.thoi_diem).concat(fc.map((x) => x.thoi_diem_muc_tieu));
    const nullA = new Array(na).fill(null), nullF = new Array(nf).fill(null);

    const thucTe = act.map((x) => x.tong_cong_suat_kw).concat(nullF);
    const duBao = nullA.slice();
    if (na) duBao[na - 1] = act[na - 1].tong_cong_suat_kw;   // nối đường dự báo vào điểm thực tế cuối
    const duBaoFull = duBao.concat(fc.map((x) => x.gia_tri_du_bao));
    const canDuoi = nullA.concat(fc.map((x) => x.can_duoi));
    const canTren = nullA.concat(fc.map((x) => x.can_tren));

    new Chart(document.getElementById('bieu-do'), {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: '_can_duoi', data: canDuoi, borderWidth: 0, pointRadius: 0, fill: false },
          { label: 'Khoảng tin cậy', data: canTren, borderWidth: 0, pointRadius: 0,
            backgroundColor: 'rgba(37,99,235,0.18)', fill: '-1' },
          { label: 'Thực tế (kW)', data: thucTe, borderColor: '#1f2937', borderWidth: 1.6, pointRadius: 0, tension: 0.15 },
          { label: 'Dự báo (kW)', data: duBaoFull, borderColor: '#2563eb', borderWidth: 2,
            borderDash: [6, 4], pointRadius: 0, tension: 0.15 },
        ],
      },
      options: tuyChonDuong('kW'),
    });
  } catch (e) { hienLoi(e); }
})();
