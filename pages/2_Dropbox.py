"""
Dropbox - upload any file, get a permanent public URL to share with an
assistant or another tool.

Storage is a separate public GitHub repo (see lib/github_io.py for why).
Files here are WORLD-READABLE. Nothing confidential goes in.
"""

import os
import sys

import requests
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import github_io as gio   # noqa: E402

st.set_page_config(page_title="Dropbox - IMP Tools", layout="wide")

PALETTE = {"blue": "#1E4FD8", "tint": "#EBF0FF", "line": "#E3E6EC",
           "muted": "#5B6478", "ink": "#12161F", "amber": "#B87800"}

st.markdown(f"""<style>
.rh-title {{ font-size:1.9rem; font-weight:700; color:{PALETTE['ink']};
             line-height:1.3; padding-top:2px; margin-bottom:.35rem; }}
.rh-sub {{ color:{PALETTE['muted']}; font-size:.92rem; margin-bottom:1.1rem; }}
.warn {{ background:#FFF8E8; border-left:3px solid {PALETTE['amber']};
         padding:.7rem .9rem; border-radius:4px; font-size:.9rem;
         margin-bottom:1rem; }}
.err {{ background:#FDEDEC; border-left:3px solid #C0392B; padding:.7rem .9rem;
        border-radius:4px; font-size:.9rem; }}
.fmeta {{ color:{PALETTE['muted']}; font-size:.82rem; }}
</style>""", unsafe_allow_html=True)

st.markdown('<div class="rh-title">Dropbox</div>', unsafe_allow_html=True)
st.markdown('<div class="rh-sub">Upload a file, get a permanent link. Share it '
            'with an assistant, another device, or another tool.</div>',
            unsafe_allow_html=True)

# -- config -------------------------------------------------------------------

try:
    C = gio.cfg(st)
except gio.ConfigError as e:
    st.markdown(f'<div class="err"><strong>Storage not configured.</strong><br>{e}'
                '<br><br>Add these in the app dashboard under Settings &rarr; '
                'Secrets:<br><code>GITHUB_TOKEN</code>, <code>GITHUB_OWNER</code>, '
                '<code>GITHUB_REPO</code>, and optionally '
                '<code>GITHUB_BRANCH</code>, <code>UPLOAD_FOLDER</code>.</div>',
                unsafe_allow_html=True)
    st.stop()

st.markdown('<div class="warn"><strong>This storage is public.</strong> Anyone '
            'with a link can read the file, and GitHub indexes public repos. '
            'Do not upload client PII, account numbers, or anything '
            'confidential.</div>', unsafe_allow_html=True)

st.markdown("**Paste this once at the start of a conversation**")
st.code(f"My file index: {gio.manifest_url(C)}\n"
        f"Fetch it to see what files I have available.", language=None)

st.divider()

# -- upload -------------------------------------------------------------------

st.markdown("### Upload")

files = st.file_uploader("Any file type", type=None, accept_multiple_files=True,
                         label_visibility="collapsed", key="dropbox_upload")

c1, c2 = st.columns([3, 2])
desc = c1.text_input("Description",
                     placeholder="e.g. MOTF advice log through 31-Jul-2026")
tag = c2.selectbox("Tag", gio.TAGS, index=gio.TAGS.index("misc"))

if files:
    total = sum(len(f.getvalue()) for f in files)
    st.caption(f"{len(files)} file(s), {gio.human(total)} total")

if st.button("Upload", type="primary", disabled=not files):
    manifest, msha = gio.load_manifest(C)
    results, prog = [], st.progress(0.0)

    for i, f in enumerate(files):
        data = f.getvalue()
        try:
            entry = gio.publish(C, data, f.name, tag=tag, description=desc,
                                manifest=manifest, manifest_sha=msha,
                                commit_manifest=False)
            results.append((f.name, entry["url"], None))
        except ValueError as e:
            results.append((f.name, None, str(e)))
        except requests.HTTPError as e:
            results.append((f.name, None, gio._api_error(e)))
        except Exception as e:
            results.append((f.name, None, f"{type(e).__name__}: {e}"))
        prog.progress((i + 1) / len(files))

    # One manifest commit for the whole batch, only if something landed.
    if any(url for _, url, _ in results):
        try:
            gio.save_manifest(C, manifest, msha)
        except Exception as e:
            st.markdown(f'<div class="err">Files uploaded but the manifest '
                        f'failed to update: {e}. The files exist; re-upload one '
                        f'small file to rebuild the index.</div>',
                        unsafe_allow_html=True)

    for name, url, err in results:
        if err:
            st.markdown(f'<div class="err"><strong>{name}</strong> - {err}</div>',
                        unsafe_allow_html=True)
        else:
            st.success(name)
            st.code(url, language=None)

st.divider()

# -- library ------------------------------------------------------------------

st.markdown("### Library")

manifest, msha = gio.load_manifest(C)
all_files = manifest.get("files", [])

if not all_files:
    st.info("Nothing uploaded yet.")
    st.stop()

present_tags = sorted({f.get("tag", "misc") for f in all_files})
f1, f2 = st.columns([2, 3])
tag_filter = f1.selectbox("Filter by tag", ["all"] + present_tags)
search = f2.text_input("Search", placeholder="filename or description")

shown = [
    f for f in all_files
    if (tag_filter == "all" or f.get("tag") == tag_filter)
    and (not search
         or search.lower() in f"{f.get('name','')} {f.get('description','')}".lower())
]

st.caption(f"Showing {len(shown)} of {len(all_files)} files "
           f"({gio.human(sum(f.get('size', 0) for f in all_files))} stored)")

for f in shown:
    with st.container(border=True):
        a, b = st.columns([6, 1])
        a.markdown(f"**{f.get('name','(unnamed)')}**")
        a.markdown(f'<div class="fmeta">{f.get("tag","misc")} &middot; '
                   f'{gio.human(f.get("size",0))} &middot; '
                   f'{f.get("uploaded","")[:10]}</div>', unsafe_allow_html=True)
        if f.get("description"):
            a.caption(f["description"])
        a.code(f.get("url", ""), language=None)

        if b.button("Delete", key=f"del_{f.get('path')}"):
            try:
                gio.remove(C, f, manifest, msha)
                st.rerun()
            except requests.HTTPError as e:
                st.markdown(f'<div class="err">Delete failed: '
                            f'{gio._api_error(e)}</div>', unsafe_allow_html=True)
            except Exception as e:
                st.markdown(f'<div class="err">Delete failed: {e}</div>',
                            unsafe_allow_html=True)
