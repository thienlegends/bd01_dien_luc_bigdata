(function () {
  const cont = document.getElementById('dags');
  let dsDag = [];

  function hangChay(r) {
    return '<tr><td>' + esc(r.dag_run_id) + '</td><td><span class="badge b-' + esc(r.state) + '">' + esc(r.state) +
      '</span></td><td>' + esc(r.run_type) + '</td><td>' + esc(fmtT(r.start_date)) + '</td><td>' + esc(fmtT(r.end_date)) + '</td></tr>';
  }

  async function taiRuns(dag) {
    const el = document.getElementById('runs-' + dag.dag_id);
    try {
      const runs = await getJSON('/api/dag/' + dag.dag_id + '/runs?gioi_han=8');
      el.innerHTML = runs.length ? runs.map(hangChay).join('') : '<tr><td colspan="5" class="ghi-chu">Chưa có lần chạy nào.</td></tr>';
    } catch (e) {
      el.innerHTML = '<tr><td colspan="5" class="ghi-chu">' + esc(e.message) + '</td></tr>';
    }
  }

  function veKhung() {
    cont.innerHTML = dsDag.map((d) => {
      const backfill = d.dag_id === 'dl_backfill'
        ? '<div class="hang"><label>Từ ngày <input type="date" id="bf-tu"></label>' +
          '<label>Đến ngày <input type="date" id="bf-den"></label></div>' : '';
      return '<section class="card"><div class="dag-ten">' + esc(d.dag_id) + '</div>' +
        '<div class="ghi-chu">' + esc(d.mo_ta) + ' · lịch: <code>' + esc(d.lich) + '</code></div>' + backfill +
        '<p><button class="btn" data-dag="' + esc(d.dag_id) + '">Chạy ngay</button> <span class="ghi-chu" id="kq-' + esc(d.dag_id) + '"></span></p>' +
        '<div class="cuon"><table><thead><tr><th>Run ID</th><th>Trạng thái</th><th>Loại</th><th>Bắt đầu</th><th>Kết thúc</th></tr></thead>' +
        '<tbody id="runs-' + esc(d.dag_id) + '"><tr><td colspan="5" class="ghi-chu">Đang tải…</td></tr></tbody></table></div></section>';
    }).join('');
    cont.querySelectorAll('button[data-dag]').forEach((b) => b.addEventListener('click', () => chay(b.dataset.dag)));
  }

  async function chay(dagId, kqId) {
    xoaLoi();
    const kq = document.getElementById(kqId || 'kq-' + dagId);
    let conf = {};
    if (dagId === 'dl_backfill') {
      conf = { start_date: document.getElementById('bf-tu').value, end_date: document.getElementById('bf-den').value };
      if (!conf.start_date || !conf.end_date) { hienLoi(new Error('Chọn khoảng ngày cho backfill.')); return; }
    }
    if (kq) kq.textContent = 'Đang gửi lệnh…';
    try {
      const r = await postJSON('/api/dag/' + dagId + '/trigger', { conf });
      if (kq) kq.textContent = 'Đã kích hoạt: ' + r.dag_run_id;
      const d = dsDag.find((x) => x.dag_id === dagId);
      if (d) taiRuns(d);
    } catch (e) { if (kq) kq.textContent = ''; hienLoi(e); }
  }

  document.getElementById('btn-du-bao').addEventListener('click', () => chay('dl_pipeline_hang_gio', 'kq-du-bao'));
  document.getElementById('btn-cache').addEventListener('click', async () => {
    const kq = document.getElementById('kq-cache');
    kq.textContent = 'Đang đồng bộ…';
    try { const r = await postJSON('/api/cache/dong-bo'); kq.textContent = r.ket_qua + ' (' + r.cap_nhat + ')'; }
    catch (e) { kq.textContent = ''; hienLoi(e); }
  });

  (async function () {
    try {
      dsDag = await getJSON('/api/dag');
      veKhung();
      dsDag.forEach(taiRuns);
      setInterval(() => dsDag.forEach(taiRuns), 10000);
    } catch (e) { hienLoi(e); }
  })();
})();
