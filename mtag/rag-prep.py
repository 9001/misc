#!/usr/bin/env python

import json
import os
import re
import subprocess as sp
import sys
import time

from typing import Any

try:
    from copyparty.util import fsenc
except:

    def fsenc(p):
        return p.encode("utf-8")


_ = r"""
for copyparty

deps:
  ffmpeg
  rclone

usage:
  -mtp x2=t5,ay,p2,kn,bin/mtag/rag-prep.py
"""


RCLONE_REMOTE = "notmybox"
CONDITIONAL_UPLOAD = True


def eprint(*a: Any, **ka: Any) -> None:
    ka["file"] = sys.stderr
    print(*a, **ka)


def log(yi: str, msg: str) -> None:
    # append to logfile
    msg = f"[{yi}] [{time.time():.3f}] {msg}"
    eprint(msg)
    with open("vlog.txt", "ab") as f:
        f.write(msg.encode("utf-8", "replace") + b"\n")


def errchk(so: bytes, se: bytes, rc: int) -> tuple[int, str]:
    if rc:
        err = (so + se).decode("utf-8", "replace").split("\n", 1)
        return rc, f"ERROR {rc}: {err[0]}"

    if se:
        err = se.decode("utf-8", "replace").split("\n", 1)
        if err:
            return rc, f"Warning: {err[0]}"

    return 0, ""


def run(cmd: list[bytes]) -> tuple[int, str]:
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
    so, se = p.communicate()
    return errchk(so, se, p.returncode)


def getmime(fp: str) -> str:
    zs = "file --mime-type"
    cmd = zs.encode("ascii").split(b" ") + [fsenc(fp)]
    so = sp.check_output(cmd)
    return so.decode("utf-8", "replace").strip().split(" ")[-1]


def fmtconv(fpi: str, fpo: str) -> tuple[int, str]:
    zs = "ffmpeg -y -hide_banner -nostdin -v warning -i"

    cmd = zs.encode("ascii").split(b" ")
    cmd += [fsenc(fpi), b"-c", b"copy", fsenc(fpo)]

    return run(cmd)


def fmtsplit(fpi: str, fpv: str, fpa: str) -> tuple[int, str]:
    zi, zv, za = [
        x.encode("ascii").split(b" ")
        for x in [
            "ffmpeg -y -hide_banner -nostdin -v warning -i",
            "-map 0:V:0 -c copy"
            + (" -movflags +faststart" if fpv.endswith("mp4") else ""),
            "-map 0:a:0 -c copy",
        ]
    ]

    ret = (0, "")
    for out_args, out_fp in [(za, fpa), (zv, fpv)]:
        cmd = zi + [fsenc(fpi)] + out_args + [fsenc(out_fp)]
        ret = run(cmd)
        if ret[0]:
            return ret

    return ret


def thumbex(fpi: str, fpo: str) -> tuple[int, str]:
    zb = [
        x.encode("ascii").split(b" ")
        for x in [
            "ffmpeg -y -hide_banner -nostdin -v warning -i",
            "-map 0:v -map -0:V -c copy",
        ]
    ]

    cmd = zb[1] + [fsenc(fpi)] + zb[2] + [fsenc(fpo)]
    return run(cmd)


def thumbgen(fpi: str, fpo: str) -> tuple[int, str]:
    zb = [
        x.encode("ascii").split(b" ")
        for x in [
            "ffmpeg -y -hide_banner -nostdin -v warning -i",
            "-map 0:v -map -0:V -vf scale=512:288:force_original_aspect_ratio=decrease,setsar=1:1 -frames:v 1 -metadata:s:v:0 rotate=0 -q:v 8",
        ]
    ]

    cmd = zb[1] + [fsenc(fpi)] + zb[2] + [fsenc(fpo)]
    return run(cmd)


def main():
    vid_fp = sys.argv[1]
    zb = sys.stdin.buffer.read()
    try:
        # prefer metadata from stdin
        zs = zb.decode("utf-8", "replace")
        md = json.loads(zs)
    except:
        # but use ffprobe if necessary
        from copyparty.mtag import ffprobe

        a, b = ffprobe(vid_fp)
        md = {k: v[1] for k, v in a.items()}

        extras = ["comment"]
        md.update({k: v[0] for k, v in b.items() if k in extras})

    yi = ""

    cmt = md.get("comment", "")
    if "youtube.com/watch?v=" in cmt:
        yi = cmt.split("v=")[1].split("&")[0]
        log(yi, f"id from comment: {vid_fp}")

    if not yi:
        subdir = vid_fp.split("/")[-2]
        if re.match(r"^[\w-]{11}$", subdir):
            yi = subdir
            log(yi, f"id from subdir: {vid_fp}")

    if not yi:
        m = re.search(r"[\[({}]([\w-]{11})[\])}][^\]\[(){}]+$", vid_fp)
        if m:
            yi = m.group(1)
            log(yi, f"id from filename: {vid_fp}")

    if not yi:
        t = "failed to determine ytid"
        log("?", f"{t}: {vid_fp}")
        return t

    if CONDITIONAL_UPLOAD:
        chk = md.get("vidchk", None)
        if chk != "ok":
            t = f"vidchk failed: {chk}"
            log(yi, f"{t}: {vid_fp}")
            return t

    # upload everything with the same basename
    ups = []
    fdir, fname = os.path.split(vid_fp)
    name = fname.rsplit(".", 1)[0] + "."
    for fn in os.listdir(fdir):
        if fn.startswith(name):
            fp = os.path.join(fdir, fn)
            log(yi, f"found {fp}")
            ups.append(fp)

    fmt = md["fmt"]
    if fmt == "matroska":
        # might be webm, ask libmagic
        fmt = getmime(vid_fp).split("-")[-1].split("/")[-1]

    log(yi, f"format: {fmt}")
    if fmt == "matroska":
        xcode_ok = False
        for ext in [".mp4", ".webm"]:
            log(yi, f"remuxing to {ext}")
            fp2 = vid_fp + ext
            rc, err = fmtconv(vid_fp, fp2)
            if rc:
                log(yi, f"remux failed; {rc}: {err}")
                try:
                    os.unlink(fp2)
                except:
                    pass
            if not rc:
                log(yi, f"remux success; {err}")
                xcode_ok = True
                ups.append(fp2)
                break

        if not xcode_ok:
            vf = "webm" if md.get("vc") == "vp8" else "mp4"
            af = "ogg" if md.get("ac") == "vorbis" else "m4a"
            log(yi, f"splitting v.{vf} a.{af}")
            fpv = f"{vid_fp}.v.{vf}"
            fpa = f"{vid_fp}.a.{af}"
            rc, err = fmtsplit(vid_fp, fpv, fpa)
            if rc:
                log(yi, f"split failed! {rc}: {err}")
                for fp2 in [fpv, fpa]:
                    try:
                        os.unlink(fp2)
                    except:
                        pass
            else:
                log(yi, "split OK")
                ups.extend([fpv, fpa])

    # TODO faststart-chk:
    # ffmpeg -v trace -hide_banner -i some.mp4 2>&1 | awk "/ type:'moov' /{a=NR} / type:'mdat' /{b=NR} END { if (a>b) { print \"ok\" }}"

    have_thumb = False
    for fp in ups:
        if fp.lower().rsplit(".")[-1] in ["jpg", "jpeg", "webp", "png"]:
            have_thumb = True

    if not have_thumb:
        for ext in ["webp", "png", "jpg"]:
            log(yi, f"thumb-ex: {ext} ...")
            fp = name + ext
            rc, err = thumbex(vid_fp, fp)
            if not rc:
                have_thumb = True
                log(yi, "thumb-ex OK")
                ups.append(fp)
                break
            else:
                log(yi, f"thumb-ex failed; {rc}: {err}")

    if not have_thumb:
        log(yi, "thumb-gen ...")
        fp = name + "jpg"
        rc, err = thumbgen(vid_fp, fp)
        if not rc:
            ups.append(fp)
            log(yi, "thumb-gen OK")
        else:
            log(yi, f"thumbing failed; {rc}: {err}")

    # skip stuff that isn't needed by the webplayer
    exts = "mp4|webm|mkv|flv|opus|ogg|mp3|m4a|aac|webp|jpg|png".split("|")
    skips = [x for x in ups if x.split(".")[-1].lower() not in exts]
    ups = [x for x in ups if x not in skips]

    # and give things better filenames
    ups2 = []  # renamed
    for fn in ups:
        fdir, fn2 = os.path.split(os.path.realpath(fn))
        ext = fn.split(".")[-1]
        suf = ""
        if ext in "mp4|webm|mkv|flv".split("|"):
            yres = md.get("res", "").split("x")[-1]
            suf = f".{yres}.{md.get('vc')}"
        fn2 = f"{yi}{suf}.{ext}"
        ups2.append(fn2)
        os.rename(fn, os.path.join(fdir, fn2))
        log(yi, f"post {fn2} = {fn.split('/')[-1]}")
    ups = ups2
    for fn in skips:
        log(yi, f"skip {fn}")

    lst = os.path.join(fdir, "rclone.lst")
    with open(lst, "w", encoding="utf-8") as f:
        f.write("\n".join(ups) + "\n")

    dst = f"{RCLONE_REMOTE}:".encode("utf-8")
    cmd = [
        b"rclone",
        b"copy",
        b"--files-from",
        lst.encode("utf-8"),
        fdir.encode("utf-8"),
    ]
    cmd += [fsenc(x) for x in ups]
    cmd += [dst]

    t0 = time.time()
    try:
        log(yi, " ".join([str(x) for x in cmd]))
        sp.check_call(cmd)
    except:
        log(yi, "rclone failed")
        sys.exit(1)

    log(yi, f"{time.time() - t0:.1f} sec")
    for fn in ups:
        os.unlink(fsenc(os.path.join(fdir, fn)))


if __name__ == "__main__":
    print(main())
