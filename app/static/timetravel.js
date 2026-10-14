(function () {
  const MAU = ['#2563eb', '#e07b00', '#2e7d32', '#8e24aa', '#c62828', '#00838f'];
  let bieuDo = null;

  async function xem() {
    xoaLoi();
    const ngay = document.getElementById('chon-ngay').value;
    if (!ngay) return;
    try {
      const d = await getJSON('/api/timetravel?ngay=' + encodeURIComponent(ngay));
      const labels = d.thuc_te.map((x) => x.thoi_diem);
      const khoa = new Set(labels);
      d.phien_ban.forEach((pb) => pb.diem.forEach((p) => khoa.add(p.thoi_diem)));
      const tatCa = Array.from(khoa).sort();
      const tra = (arr, kk) => { const m = new Map(arr.map((p) => [p.thoi_diem, p[kk]])); return tatCa.map((t) => (m.has(t) ? m.get(t) : null)); };

      const datasets = [{ label: 'Thực tế', data: tra(d.thuc_te, 'gia_tri'), borderColor: '#1f2937', borderWidth: 2.2, pointRadius: 0 }];
      d.phien_ban.forEach((pb, i) => datasets.push({
        label: 'Phiên bản ' + pb.phien_ban, data: tra(pb.diem, 'gia_tri'),
        borderColor: MAU[i % MAU.length], borderWidth: 1.6, borderDash: [5, 3], pointRadius: 0,
      }));
      if (bieuDo) bieuDo.destroy();
      bieuDo = new Chart(document.getElementById('bieu-do'), {
        type: 'line', data: { labels: tatCa, datasets }, options: tuyChonDuong('kW'),
      });

      document.querySelector('#bang-pb tbody').innerHTML = d.phien_ban.map((pb) =>
        '<tr><td>' + esc(pb.phien_ban) + '</td><td>' + esc(fmtT(pb.thoi_diem_du_bao)) + '</td><td>' +
        pb.so_diem + '</td><td>' + fmtPct(pb.mape) + '</td></tr>').join('');
      document.querySelector('#bang-sl tbody').innerHTML = d.so_sanh.map((s) =>
        '<tr><td>' + esc(s.tu_phien_ban) + '</td><td>' + esc(s.den_phien_ban) + '</td><td>' + s.so_diem_chung +
        '</td><td>' + fmtKW(s.sai_lech_tb_kw) + '</td><td>' + fmtPct(s.sai_lech_tb_pct) + '</td></tr>').join('');

      const t = d.du_lieu_tre;
      document.getElementById('thong-ke').textContent = t
        ? 'Ngày ' + d.ngay + ': ' + t.so_ban_ghi_tre + '/' + t.tong_ban_ghi + ' bản ghi đến trễ ≥ 1 giờ, ' +
          t.so_hieu_chinh + ' bản ghi đã hiệu chỉnh.'
        : (d.phien_ban.length < 2 ? 'Ngày này chỉ có một phiên bản dự báo, chưa so sánh được.' : '');
    } catch (e) { hienLoi(e); }
  }

  (async function () {
    try {
      const ds = await getJSON('/api/timetravel/ngay');
      const sel = document.getElementById('chon-ngay');
      sel.innerHTML = ds.map((x) => '<option value="' + esc(x.ngay) + '">' + esc(x.ngay) +
        ' (' + x.so_phien_ban + ' phiên bản)</option>').join('');
      // ưu tiên chọn ngày có từ 2 phiên bản trở lên
      const co = ds.find((x) => x.so_phien_ban >= 2);
      if (co) sel.value = co.ngay;
      document.getElementById('btn-xem').addEventListener('click', xem);
      xem();
    } catch (e) { hienLoi(e); }
  })();
})();
