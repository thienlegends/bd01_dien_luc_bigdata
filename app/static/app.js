/* Hàm dùng chung cho mọi trang */
async function loiTuResponse(r) {
  try {
    const j = await r.json();
    return typeof j.detail === 'string' ? j.detail : JSON.stringify(j.detail);
  } catch (e) {
    return r.status + ' ' + r.statusText;
  }
}
async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await loiTuResponse(r));
  return r.json();
}
async function postJSON(url, body) {
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {}),
  });
  if (!r.ok) throw new Error(await loiTuResponse(r));
  return r.json();
}
function esc(s) {
  const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => map[c]);
}
function hienLoi(e) {
  const el = document.getElementById('loi');
  el.textContent = e && e.message ? e.message : String(e);
  el.hidden = false;
}
function xoaLoi() { document.getElementById('loi').hidden = true; }
function fmtT(iso) { return iso ? String(iso).replace('T', ' ').slice(0, 16) : ''; }
function fmtKW(v) {
  return v == null ? '–' : Number(v).toLocaleString('vi-VN', { maximumFractionDigits: 1 });
}
function fmtPct(v) { return v == null ? '–' : Number(v).toFixed(2) + '%'; }

const TEN_TRANG_THAI = { moi: 'Mới', dang_kiem_tra: 'Đang kiểm tra', xac_nhan: 'Đã xác nhận', bo_qua: 'Bỏ qua' };
const TEN_LOAI = { sut_giam: 'Sụt giảm', tang_vot: 'Tăng vọt', lech_cum: 'Lệch cụm', mat_tin_hieu: 'Mất tín hiệu' };
function badge(tt) {
  return '<span class="badge b-' + esc(tt) + '">' + esc(TEN_TRANG_THAI[tt] || tt) + '</span>';
}

/* Tuỳ chọn Chart.js chung: trục X dạng chuỗi thời gian ISO */
function tuyChonDuong(donVi) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: {
        ticks: {
          maxTicksLimit: 12, maxRotation: 0,
          callback: function (v) { return fmtT(this.getLabelForValue(v)).slice(5, 16); },
        },
      },
      y: { title: { display: true, text: donVi || 'kW' } },
    },
    plugins: {
      legend: { labels: { filter: (item) => !String(item.text).startsWith('_') } },
      tooltip: { callbacks: { title: (items) => fmtT(items[0].label) } },
    },
  };
}
