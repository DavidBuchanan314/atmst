import sys
import os
import base64
import json
from typing import Tuple, BinaryIO

from cbrrr import encode_dag_cbor, decode_dag_cbor, CID

from .blockstore.car_file import ReadOnlyCARBlockStore, encode_varint
from .blockstore import OverlayBlockStore
from .mst.node_store import NodeStore
from .mst.node_walker import NodeWalker
from .mst.diff import mst_diff, record_diff


def prettify_record(record) -> str:
	return json.dumps(record, indent="  ")

def open_car(car_path: str) -> Tuple[ReadOnlyCARBlockStore, dict]:
	carfile = open(car_path, "rb")
	bs = ReadOnlyCARBlockStore(carfile)
	commit = decode_dag_cbor(bs.get_block(bytes(bs.car_root)))
	return bs, commit

def print_info(car_path: str) -> None:
	print(f"Reading {car_path!r}")
	print(f"Size on disk: {os.stat(car_path).st_size} bytes")
	bs, commit = open_car(car_path)
	print("Total CAR blocks:", len(bs.block_offsets))
	print("Root CID:", bs.car_root.encode("base32"))
	print()
	print("ATProto commit info:")
	print("Version:", commit["version"])
	if commit["version"] != 3:
		print(f"Error: only v3 repo format is supported. Got:", commit["version"])
		return
	print("Repo:", commit["did"])
	print("Rev:", commit["rev"])
	print("Sig:", base64.urlsafe_b64encode(commit["sig"]).decode())
	print("MST root:", commit["data"].encode("base32"))

def print_all_records(car_path: str, to_json: bool) -> None:
	bs, commit = open_car(car_path)
	for k, v in NodeWalker(NodeStore(bs), commit["data"]).iter_kv():
		if to_json:
			record = decode_dag_cbor(bs.get_block(bytes(v)), atjson_mode=True)
			print(f"{json.dumps(k)} -> {prettify_record(record)}")
		else:
			print(f"{json.dumps(k)} -> {v.encode('base32')}")

def list_all(car_path: str):
	print_all_records(car_path, to_json=False)

def dump_all(car_path: str):
	sys.setrecursionlimit(99999999) # allow printing very deeply nested records
	print_all_records(car_path, to_json=True)

def dump_record(car_path: str, key: str):
	bs, commit = open_car(car_path)
	val = NodeWalker(NodeStore(bs), commit["data"]).find_value(key)
	if val is None:
		print("Record not found!", file=sys.stderr)
		sys.exit(-1)
	record = decode_dag_cbor(bs.get_block(bytes(val)), atjson_mode=True)
	print(prettify_record(record))

def write_block(file: BinaryIO, data: bytes) -> None:
	file.write(encode_varint(len(data)))
	file.write(data)

def compact(car_in: str, car_out: str):
	bs, commit = open_car(car_in)
	with open(car_out, "wb") as carfile_out:
		new_header = encode_dag_cbor({
			"version": 1,
			"roots": [bs.car_root]
		})
		write_block(carfile_out, new_header)
		write_block(carfile_out, bytes(bs.car_root) + encode_dag_cbor(commit))
		dedup = {bs.car_root}

		for node in NodeWalker(NodeStore(bs), commit["data"]).iter_nodes():
			if node.cid not in dedup:
				write_block(carfile_out, bytes(node.cid) + node.serialised)
				dedup.add(node.cid)
			for v in node.vals:
				if v not in dedup:
					write_block(carfile_out, bytes(v) + bs.get_block(bytes(v)))
					dedup.add(v)

def _delta_str(a: str, b: str):
	if a == b:
		return f"{a} == {b}"
	return f"{a} -> {b}"

def print_record_diff(car_a: str, car_b: str):
	bs_a, commit_a = open_car(car_a)
	bs_b, commit_b = open_car(car_b)
	print(f"Repo: {_delta_str(commit_a['did'], commit_b['did'])}")
	print(f"Revision: {_delta_str(commit_a['rev'], commit_b['rev'])}")
	print(f"Commit: {_delta_str(bs_a.car_root.encode('base32'), bs_b.car_root.encode('base32'))}")
	print(f"MST root: {_delta_str(commit_a['data'].encode('base32'), commit_b['data'].encode('base32'))}")
	print("")
	print("Record delta:")
	bs = OverlayBlockStore(bs_a, bs_b)
	ns = NodeStore(bs)
	mst_created, mst_deleted = mst_diff(ns, commit_a["data"], commit_b["data"])
	for delta in record_diff(ns, mst_created, mst_deleted):
		print(delta)

COMMANDS = {
	"info": (print_info, "print CAR header and repo info"),
	"list": (list_all, "list all records in the CAR (values as CIDs)"),
	"dump": (dump_all, "dump all records in the CAR (values as JSON)"),
	"dump_record": (dump_record, "dump a single record, keyed on ('collection/rkey')"),
	"compact": (compact, "rewrite the whole CAR, dropping any duplicated or unreferenced blocks"),
	"diff": (print_record_diff, "list the record diff between two CAR files")
}

def print_help():
	print("USAGE: cartool COMMAND [args...]")
	print("")
	print("Available commands:")
	for cmdname, (cmdfn, helptext) in COMMANDS.items():
		fn_args = cmdfn.__code__.co_varnames[:cmdfn.__code__.co_argcount]
		args = [f"<{arg}>" for arg in fn_args]
		print(f"{cmdname} {' '.join(args)} : {helptext}")

def main():
	if len(sys.argv) < 2:
		print_help()
		return

	command, *args = sys.argv[1:]
	COMMANDS[command][0](*args)

if __name__ == "__main__":
	main()
