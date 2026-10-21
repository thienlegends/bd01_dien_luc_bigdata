(function () {
  const ma = document.getElementById('app').dataset.ma;
  let canhBao = null;
  let bieuDo = null;
  let timer = null;

  function veThongTin(c) {
    const bc = c.bang_chung && typeof c.bang_chung === 'object'
      ? Object.entries(c.bang_chung).map(([k, v]) => '<tr><td>' + esc(k) + '</td><td>' + esc(v) + '</td></tr>').join('')
      : '';
    document.getElementById('thong-tin').innerHTML =
      '<h2>Khách hàng ' + esc(c.ma_khach_hang) + '</h2>' +
      '<p>Loại: <b>' + esc(TEN_LOAI[c.loai] || c.loai) + '</b> · Điểm bất thường: <b>' +
      (c.diem_bat_thuong == null ? '–' : Number(c.diem_bat_thuong).toFixed(2)) + '</b> · Ngày phát hiện: ' +
      esc(c.ngay_phat_hien) + ' · Trạng thái: <span id="tt-hien-tai">' + badge(c.trang_thai) + '</span></p>' +
      (bc ? '<div class="cuon"><table><thead><tr><th>Bằng chứng</th><th>Giá trị</th></tr></thead><tbody>' + bc + '</tbody></table></div>' : '');
  }

  function veBieuDo(diem) {
    const labels = diem.map((x) => x.thoi_diem_doc);
    const v = diem.map((x) => x.cong_suat_kw);
    const lo = diem.map((x) => x.bao_duoi), hi = diem.map((x) => x.bao_tren);
    const ngoai = v.map((y, i) => y != null && lo[i] != null && hi[i] != null && (y < lo[i] || y > hi[i]));
    bieuDo = new Chart(document.getElementById('bieu-do'), {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: '_bao_duoi', data: lo, borderWidth: 0, pointRadius: 0, fill: false },
          { label: 'Vùng bình thường (5%–95%)', data: hi, borderWidth: 0, pointRadius: 0,
            backgroundColor: 'rgba(46,125,50,0.18)', fill: '-1' },
          { label: 'Tiêu thụ (kW)', data: v, borderColor: '#1f2937', borderWidth: 1.4, tension: 0.1,
            pointRadius: ngoai.map((o) => (o ? 3 : 0)),
            pointBackgroundColor: ngoai.map((o) => (o ? '#c62828' : 'transparent')),
            pointBorderColor: ngoai.map((o) => (o ? '#c62828' : 'transparent')) },
        ],
      },
      options: tuyChonDuong('kW'),
    });
  }

  async function theoDoiDongBo() {
    clearTimeout(timer);
    try {
      const s = await getJSON('/api/canh-bao/' + encodeURIComponent(ma) + '/dong-bo');
      const el = document.getElementById('ket-qua');
      if (!s.co_ban_ghi) return;
      if (s.da_dong_bo === 1) el.textContent = '✔ Đã ghi vào Delta (silver.xac_nhan_canh_bao) và cập nhật gold.canh_bao_bat_thuong.';
      else if (s.da_dong_bo === 2) el.textContent = 'Đã lưu trên web (đồng bộ Delta đang tắt).';
      else if (s.da_dong_bo === -1) el.textContent = '⚠ Ghi Delta lỗi: ' + (s.thong_bao || '');
      else { el.textContent = 'Đã ghi nhận, đang đồng bộ vào Delta…'; timer = setTimeout(theoDoiDongBo, 4000); }
    } catch (e) { /* bỏ qua lỗi tạm thời khi poll */ }
  }

  async function gui(ketLuan, btn) {
    xoaLoi();
    const nguoi = document.getElementById('nguoi').value.trim();
    if (!nguoi) { hienLoi(new Error('Hãy nhập tên người xử lý.')); return; }
    localStorage.setItem('nguoi_xu_ly', nguoi);
    document.querySelectorAll('.nut button').forEach((b) => (b.disabled = true));
    try {
      const r = await postJSON('/api/canh-bao/' + encodeURIComponent(ma) + '/ketluan',
        { nguoi_xu_ly: nguoi, ket_luan: ketLuan, ghi_chu: document.getElementById('ghi-chu').value });
      document.getElementById('tt-hien-tai').innerHTML = badge(r.trang_thai);
      document.getElementById('ket-qua').textContent = r.ghi_chu;
      theoDoiDongBo();
    } catch (e) { hienLoi(e); }
    document.querySelectorAll('.nut button').forEach((b) => (b.disabled = false));
  }

  document.querySelectorAll('.nut button').forEach((b) =>
    b.addEventListener('click', () => gui(b.dataset.kl, b)));
  document.getElementById('nguoi').value = localStorage.getItem('nguoi_xu_ly') || '';

  (async function () {
    try {
      canhBao = await getJSON('/api/canh-bao/' + encodeURIComponent(ma));
      veThongTin(canhBao);
      const tt = await getJSON('/api/khach-hang/' + encodeURIComponent(canhBao.ma_khach_hang) + '/tieu-thu?ngay=10');
      veBieuDo(tt.diem);
      if (canhBao.dong_bo && canhBao.dong_bo.co_ban_ghi) theoDoiDongBo();
    } catch (e) { hienLoi(e); }
  })();
})();
