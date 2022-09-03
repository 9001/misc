#!/usr/bin/env python

import base64
import hashlib
import json
import os
import random
import re
import sqlite3
import subprocess as sp
import sys
import time

from datetime import datetime
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
  mediainfo

usage:
  -mtp x2=t5,ay,p2,kn,bin/mtag/rag-prep.py
"""


RCLONE_REMOTE = "notmybox"
CONDITIONAL_UPLOAD = True
DRYRUN = False
DEBOUNCE = 2 if DRYRUN else 10


def eprint(*a: Any, **ka: Any) -> None:
    ka["file"] = sys.stderr
    print(*a, **ka)


def log(yi: str, msg: str) -> None:
    # append to logfile
    ts = datetime.utcnow().strftime("%Y-%m%d-%H%M%S.%f")[:-3]
    msg = f"[{yi}] [{ts}] {msg}"
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


def fmtconv(fpi: str, fpo: str, no_att="") -> tuple[int, str]:
    zi, zo = [
        x.encode("ascii").split(b" ")
        for x in [
            "ffmpeg -y -hide_banner -nostdin -v warning -i",
            f"-map 0 {no_att} -c copy -movflags +faststart",
        ]
    ]

    a, b = os.path.split(fpo)
    tfpo = os.path.join(a, "mux-" + b)
    cmd = zi + [fsenc(fpi)] + zo + [fsenc(tfpo)]

    ret = run(cmd)
    if not ret[0]:
        os.rename(tfpo, fpo)
    else:
        try:
            os.unlink(tfpo)
        except:
            pass

    return ret


def fmtsplit(yi, fpi: str, fpv: str, fpa: str) -> tuple[int, str]:
    vf = " -movflags +faststart" if fpv.endswith("mp4") else ""
    af = " -movflags +faststart" if fpa.endswith("m4a") else ""
    zi, zv, za = [
        x.encode("ascii").split(b" ")
        for x in [
            "ffmpeg -y -hide_banner -nostdin -v warning -i",
            "-map 0:V:0 -map -0:t -c copy" + vf,
            "-map 0:a:0 -map -0:t -c copy" + af,
        ]
    ]

    ret = (0, "")
    for out_args, out_fp in [(za, fpa), (zv, fpv)]:
        cmd = zi + [fsenc(fpi)] + out_args + [fsenc(out_fp)]
        # log(yi, str(cmd))
        ret = run(cmd)
        if ret[0]:
            return ret

    return ret


def thumbex(fpi: str, fpo: str) -> tuple[int, str]:
    zi, zo = [
        x.encode("ascii").split(b" ")
        for x in [
            "ffmpeg -y -hide_banner -nostdin -v warning -i",
            "-map 0:v -map -0:V -c copy",
        ]
    ]

    cmd = zi + [fsenc(fpi)] + zo + [fsenc(fpo)]
    return run(cmd)


def thumbgen(fpi: str, fpo: str) -> tuple[int, str]:
    zi, zo = [
        x.encode("ascii").split(b" ")
        for x in [
            "ffmpeg -y -hide_banner -nostdin -v warning -i",
            "-map 0:V -vf scale=512:288:force_original_aspect_ratio=decrease,setsar=1:1 -frames:v 1 -metadata:s:v:0 rotate=0 -q:v 8",
        ]
    ]

    cmd = zi + [fsenc(fpi)] + zo + [fsenc(fpo)]
    return run(cmd)


def esdoc_from_ffprobe(yi, md, mig):
    log(yi, "esdoc from ffprobe...")
    md_map = {
        "channel_name": "artist",
        "title": "title",
        "upload_date": "date",
        "description": "description",
        "duration": ".dur",
        "width": ".resw",
        "height": ".resh",
        "fps": ".fps",
    }

    uploaded = md.get("date")
    if uploaded:
        m = re.search(r"^(....)-?(..)-?(..)", str(uploaded))
        if m:
            md["date"] = f"{m[1]}-{m[2]}-{m[3]}"

    doc = {"video_id": yi}
    for k, ffk in md_map.items():
        v = md.get(ffk)
        if v:
            try:
                doc[k] = int(round(float(v))) if ffk.startswith(".") else v
            finally:
                pass

    try:
        doc["description"] = mig["Description"]
    except:
        pass

    return doc


def esdoc_from_infojson(yi, vid_fp, ups):
    log(yi, "esdoc from infojson...")
    zs = "ffprobe -hide_banner -show_streams -show_format -of json"
    cmd = zs.encode("ascii").split(b" ") + [fsenc(vid_fp)]
    so = sp.check_output(cmd)
    fj = json.loads(so.decode("utf-8", "replace"))
    p1 = None  # found by filename
    p2 = None  # found by mimetype
    # log(yi, vid_fp + "\n" + json.dumps(fj))
    for st in fj["streams"]:
        try:
            if st["tags"]["filename"].lower().endswith(".info.json"):
                p1 = st["index"]
                break
            if st["tags"]["mimetype"].lower() == "application/json":
                p2 = st["index"]
        except:
            pass

    n = p2 if p1 is None else p1
    if n is None:
        return {}

    log(yi, f"found infojson at #{p1}, #{p2}")
    if os.path.exists("i.json"):
        os.unlink("i.json")

    zi, zo = [
        x.encode("ascii").split(b" ")
        for x in [
            f"ffmpeg -hide_banner -dump_attachment:{n} i.json -i",
            "-c copy -t 1 -f null -",
        ]
    ]

    rc, err = run(zi + [fsenc(vid_fp)] + zo)
    if rc:
        raise Exception(f"json extract failed: {err}")

    with open("i.json", "r", encoding="utf-8") as f:
        infojson = json.load(f)

    uploaded = str(infojson["upload_date"])
    m = re.search(r"^(....)-?(..)-?(..)", uploaded)
    if m:
        uploaded = f"{m[1]}-{m[2]}-{m[3]}"

    # remaining code below stolen from aa.dw
    files = []
    for filepath in ups:
        filename = os.path.basename(filepath)
        size = os.path.getsize(filepath)
        files.append({"name": filename, "size": size})

    esdoc = {
        "video_id": infojson["id"],
        "channel_name": infojson["uploader"],
        "channel_id": infojson["channel_id"],
        "upload_date": uploaded,
        "title": infojson["title"],
        "description": infojson["description"],
        "duration": infojson["duration"],
        "width": infojson["width"],
        "height": infojson["height"],
        "fps": infojson["fps"],
        "format_id": infojson["format_id"],
        "view_count": infojson["view_count"],
        "like_count": infojson.get("like_count", -1),
        "dislike_count": infojson.get("dislike_count", -1),
        "files": files,
        # "drive_base": ROOT_FOLDER_ID,
        # "archived_timestamp": datetime.datetime.utcnow().isoformat(),
        # "timestamps": youtube_fetch_timestamps(infojson["id"]),
    }
    return esdoc


def write_esdoc(yi, vid_fp, ups, md, mig):
    # if '.info.json"' in json.dumps(mig.get("extra", {})):
    try:
        doc = esdoc_from_infojson(yi, vid_fp, ups)
    except Exception as ex:
        doc = {}
        log(yi, f"esdoc from infojson failed: {ex}")

    if not doc:
        doc = esdoc_from_ffprobe(yi, md, mig)

    ip = md.get("up_ip")
    ts = md.get("up_at")
    if ip:
        db = sqlite3.connect("guestbook.db3")
        t = "select msg from gb where ip = ? order by ts desc"
        r = db.execute(t, (md["up_ip"],)).fetchone()
        db.close()

        if r:
            uid = r[0]
        else:
            uid = ip
            if os.path.exists("salt"):
                with open("salt", "r") as f:
                    salt = f.read()
            else:
                salt = base64.b64encode(os.urandom(32)).decode("ascii")[:24]
                with open("salt", "w") as f:
                    f.write(salt)
            uid = ip + salt
            buid = hashlib.sha1(uid.encode("ascii")).digest()
            uid = "ip:" + base64.b64encode(buid).decode("ascii")[:24]

        log(yi, f"uploader: {ip} = {uid}")

        doc["import"] = {
            "is_imported": True,  # a
            "received_at": ts,
            "received_from": uid,
        }

    os.makedirs("esdocs", exist_ok=True)
    with open(f"esdocs/{yi}.json", "w", encoding="utf-8") as f:
        json.dump(doc, f, indent="  ")

    log(yi, "esdoc ok")


def main():
    vid_fp = sys.argv[1]

    fdir = os.path.dirname(os.path.realpath(vid_fp))
    flag = os.path.join(fdir, ".processed")
    if os.path.exists(flag):
        return "already processed"

    # wait until folder idle
    while True:
        busy = False
        for _ in range(DEBOUNCE):
            time.sleep(1)
            for f in os.listdir(fdir):
                if f.endswith(".PARTIAL"):
                    busy = True

            if busy:
                break

        if not busy:
            break

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

    subdir = vid_fp.split("/")[-2]
    if re.match(r"^[\w-]{11}-[0-9]{13}$", subdir):
        if not yi:
            yi = subdir[:11]
            log(yi, f"id from subdir: {vid_fp}")
        with open(flag, "w") as f:
            f.write("a")

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

    mib = sp.check_output([b"mediainfo", b"--Output=JSON", b"--", fsenc(vid_fp)])
    mi = json.loads(mib.decode("utf-8", "replace"))
    mig = next(x for x in mi["media"]["track"] if x["@type"] == "General")
    miv = next((x for x in mi["media"]["track"] if x["@type"] == "Video"), None)
    fmt = mig["Format"].lower()
    log(yi, f"format: {fmt}")

    ext = None
    need_remux = False
    if fmt == "matroska":
        ext = "mkv"
        need_remux = True
    elif fmt == "webm":
        ext = "webm"
    elif fmt == "mpeg-4":
        ext = "mp4" if miv else "m4a"
        need_remux = mig.get("IsStreamable") != "Yes"
    elif fmt == "flash video":
        ext = "flv"

    if ext and not vid_fp.lower().endswith("." + ext):
        fn2 = vid_fp.rsplit(".", 1)[0] + "." + ext
        os.rename(vid_fp, fn2)
        log(yi, f"renamed {vid_fp} => {fn2}")
        vid_fp = fn2

    # upload everything with the same basename
    ups = []
    fdir, fname = os.path.split(vid_fp)
    name = fname.rsplit(".", 1)[0] + "."
    for fn in os.listdir(fdir):
        if fn.startswith(name):
            fp = os.path.join(fdir, fn)
            log(yi, f"found {fp}")
            ups.append(fp)

    if need_remux:
        remux_ok = False
        for no_att in ["", "-map -0:t"]:
            for ext in [".mp4", ".webm"]:
                log(yi, f"remuxing to {ext}")
                fp2 = vid_fp + ext
                rc, err = fmtconv(vid_fp, fp2, no_att)
                if rc:
                    log(yi, f"remux failed; {rc}: {err}")
                    try:
                        os.unlink(fp2)
                    except:
                        pass
                if not rc:
                    log(yi, f"remux success; {err}")
                    remux_ok = True
                    ups.append(fp2)
                    break

            if remux_ok:
                break

        if not remux_ok:
            vf = "webm" if md.get("vc") == "vp8" else "mp4"
            af = "ogg" if md.get("ac") in ["vorbis", "opus"] else "m4a"
            log(yi, f"splitting v.{vf} a.{af}")
            fpv = f"{vid_fp}.v.{vf}"
            fpa = f"{vid_fp}.a.{af}"
            rc, err = fmtsplit(yi, vid_fp, fpv, fpa)
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

    have_thumb = False
    for fp in ups:
        if fp.lower().rsplit(".")[-1] in ["jpg", "jpeg", "webp", "png"]:
            have_thumb = True

    if not have_thumb:
        for ext in [".webp", ".png", ".jpg"]:
            log(yi, f"thumb-ex: {ext} ...")
            fp = vid_fp + ext
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
        fp = vid_fp + ".jpg"
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
    for fp in ups:
        fn2 = os.path.basename(os.path.realpath(fp))
        ext = fp.split(".")[-1]
        suf = ""
        if ext in "mp4|webm|mkv|flv".split("|"):
            yres = md.get("res", "").split("x")[-1]
            suf = f".{yres}.{md.get('vc')}"
        fn2 = f"{yi}{suf}.{ext}"
        ups2.append(fn2)
        log(yi, f"post {fn2} = {fp.split('/')[-1]}")
        fp2 = os.path.join(fdir, fn2)
        os.rename(fp, fp2)
        if vid_fp == fp:
            vid_fp = fp2
    ups = ups2
    for fn in skips:
        log(yi, f"skip {fn}")

    write_esdoc(yi, vid_fp, [os.path.join(fdir, x) for x in ups], md, mig)

    lst = os.path.join(fdir, "rclone.lst")
    with open(lst, "w", encoding="utf-8") as f:
        f.write("\n".join(ups) + "\n")

    dst = f"{RCLONE_REMOTE}:{yi}/".encode("utf-8")
    cmd = [
        b"rclone",
        b"copy",
        b"--files-from",
        lst.encode("utf-8"),
        fdir.encode("utf-8"),
    ]
    cmd += [dst]

    t0 = time.time()
    try:
        log(yi, " ".join([str(x) for x in cmd]))
        if not DRYRUN:
            sp.check_call(cmd)
    except:
        log(yi, "rclone failed")
        sys.exit(1)

    log(yi, f"{time.time() - t0:.1f} sec")
    for fn in ups:
        if not DRYRUN:
            os.unlink(fsenc(os.path.join(fdir, fn)))


if __name__ == "__main__":
    print(main())
