(function () {
  const tbody = document.querySelector('#bang tbody');

  function tomTatBangChung(bc) {
    if (!bc || typeof bc !== 'object') return '';
    return Object.entries(bc).slice(0, 3).map(([k, v]) => esc(k) + ': ' + esc(v)).join(' · ');
  }

  async function tai() {
    xoaLoi();
    const q = new URLSearchParams();
    const loai = document.getElementById('f-loai').value;
    const tt = document.getElementById('f-tt').value;
    const diem = document.getElementById('f-diem').value;
    if (loai) q.set('loai', loai);
    if (tt) q.set('trang_thai', tt);
    if (diem) q.set('diem_min', diem);
    try {
      const rows = await getJSON('/api/canh-bao?' + q.toString());
      document.getElementById('dem').textContent = rows.length + ' cảnh báo';
      tbody.innerHTML = rows.map((r) =>
        '<tr class="bam" data-ma="' + esc(r.ma_canh_bao) + '">' +
        '<td>' + esc(r.ma_khach_hang) + '</td>' +
        '<td>' + esc(r.ngay_phat_hien) + '</td>' +
        '<td>' + esc(TEN_LOAI[r.loai] || r.loai) + '</td>' +
        '<td><span class="diem">' + (r.diem_bat_thuong == null ? '–' : Number(r.diem_bat_thuong).toFixed(2)) + '</span></td>' +
        '<td>' + badge(r.trang_thai) + '</td>' +
        '<td class="ghi-chu">' + tomTatBangChung(r.bang_chung) + '</td></tr>').join('');
    } catch (e) { hienLoi(e); }
  }

  tbody.addEventListener('click', (ev) => {
    const tr = ev.target.closest('tr[data-ma]');
    if (tr) location.href = '/canh-bao/' + encodeURIComponent(tr.dataset.ma);
  });
  document.getElementById('btn-loc').addEventListener('click', tai);
  tai();
})();
