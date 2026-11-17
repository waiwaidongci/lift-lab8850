"""结果产物的原子写入与完整性校验。

一次计算对应一个独立的 attempt 文件；只有先完成 fsync + 原子重命名，并把
SHA256 与大小写入分析记录，结果才对读取方可见。迟到的旧进程只会写自己的
attempt 文件，无法覆盖接管进程的文件或记录。
"""
import hashlib
import json
import os
from pathlib import Path

from ..config.settings import ARTIFACT_DIR


def _root() -> Path:
    return Path(ARTIFACT_DIR)


def relative_path(run_id: int, attempt: int) -> str:
    return f"run-{run_id}/attempt-{attempt}.json"


def _absolute(relative: str) -> Path:
    root = _root().resolve()
    target = (root / relative).resolve()
    if target != root and root not in target.parents:
        raise ValueError("产物路径越界")
    return target


def payload(run_id: int, experiment_id: int, attempt: int, result: dict, completed_at: str) -> dict:
    return {
        "schema": 1,
        "run_id": run_id,
        "experiment_id": experiment_id,
        "attempt": attempt,
        "completed_at": completed_at,
        "result": result,
    }


def commit(run_id: int, experiment_id: int, attempt: int, result: dict, completed_at: str) -> dict:
    """写临时文件 → fsync → 原子替换，返回可持久化的产物元数据。"""
    relative = relative_path(run_id, attempt)
    target = _absolute(relative)
    target.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(
        payload(run_id, experiment_id, attempt, result, completed_at),
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    ).encode("utf-8")
    digest = hashlib.sha256(body).hexdigest()
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        with open(temporary, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink(missing_ok=True)
    return {"artifact_path": relative, "artifact_sha256": digest, "artifact_size": len(body)}


def read(relative: str, sha256: str, size: int) -> dict | None:
    """读取并校验产物；任何不一致都返回 None。"""
    try:
        target = _absolute(relative)
        body = target.read_bytes()
    except (OSError, ValueError):
        return None
    if len(body) != int(size):
        return None
    if hashlib.sha256(body).hexdigest() != sha256:
        return None
    try:
        return json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def verify(relative: str, run_id: int, experiment_id: int, attempt: int, sha256: str, size: int) -> dict | None:
    """确认文件、哈希与分析记录一一对应。"""
    data = read(relative, sha256, size)
    if not data:
        return None
    if data.get("schema") != 1:
        return None
    if data.get("run_id") != run_id or data.get("experiment_id") != experiment_id or data.get("attempt") != attempt:
        return None
    if not isinstance(data.get("result"), dict) or not data["result"]:
        return None
    return data


def prune_other_attempts(run_id: int, keep_relative: str | None) -> None:
    """清掉同一分析被接管前遗留的 attempt 文件，只保留最终产物。

    迟到的旧进程可能再次写出自己的 attempt 文件，但它不会成为记录所指向的
    产物。清理时可能与接管进程的写入并发：先写临时文件再原子重命名，因此被
    清掉的只可能是旧内容；接管进程随后重新写出的文件仍可正常登记。
    """
    directory = (_root() / f"run-{run_id}").resolve()
    keep = _absolute(keep_relative).resolve() if keep_relative else None
    try:
        for item in directory.iterdir():
            if not item.is_file() or item.resolve() == keep:
                continue
            if item.name.endswith(".tmp"):
                # 临时文件属于并发写入者，交给写入方自己的 finally 清理。
                continue
            item.unlink(missing_ok=True)
    except OSError:
        pass


def discard(relative: str) -> None:
    """删除某个确定不被记录引用的 attempt 文件（旧进程迟到结果）。"""
    try:
        _absolute(relative).unlink(missing_ok=True)
    except (OSError, ValueError):
        pass
