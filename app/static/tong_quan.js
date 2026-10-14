(async function () {
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
