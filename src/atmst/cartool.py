import sys
import os
import base64
import json

import dag_cbor
from multiformats import CID

from .blockstore.car_reader import ReadOnlyCARBlockStore
from .mst.node_store import NodeStore
from .mst.node_walker import NodeWalker


class ATJsonEncoder(json.JSONEncoder):
	def default(self, o):
		if isinstance(o, bytes):
			return {"$bytes": base64.b64encode(o).decode()}
		if isinstance(o, CID):
			return {"$link": o.encode("base32")}
		return json.JSONEncoder.default(self, o)

def prettify_record(record) -> str:
	return json.dumps(record, indent="  ", cls=ATJsonEncoder)

def print_info(car_path: str) -> None:
	print(f"Reading {car_path!r}")
	print(f"Size on disk: {os.stat(car_path).st_size} bytes")
	with open(car_path, "rb") as carfile:
		bs = ReadOnlyCARBlockStore(carfile)
		print("Total CAR blocks:", len(bs.block_offsets))
		print("Root CID:", bs.car_root.encode("base32"))
		commit = dag_cbor.decode(bs.get_block(bytes(bs.car_root)))
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
	with open(car_path, "rb") as carfile:
		bs = ReadOnlyCARBlockStore(carfile)
		ns = NodeStore(bs)
		commit = dag_cbor.decode(bs.get_block(bytes(bs.car_root)))
		nw = NodeWalker(ns, commit["data"])
		for k, v in nw.iter_kv():
			if to_json:
				record = dag_cbor.decode(bs.get_block(bytes(v)))
				print(f"{k} -> {prettify_record(record)}")
			else:
				print(f"{k} -> {v.encode('base32')}")

def list_all(car_path: str):
	print_all_records(car_path, to_json=False)

def dump_all(car_path: str):
	sys.setrecursionlimit(99999999) # allow printing very deeply nested records
	print_all_records(car_path, to_json=True)

def dump_record(car_path: str, key: str):
	with open(car_path, "rb") as carfile:
		bs = ReadOnlyCARBlockStore(carfile)
		ns = NodeStore(bs)
		commit = dag_cbor.decode(bs.get_block(bytes(bs.car_root)))
		nw = NodeWalker(ns, commit["data"])
		val = nw.find_value(key)
		if val is None:
			print("Record not found!", file=sys.stderr)
			sys.exit(-1)
		record = dag_cbor.decode(bs.get_block(bytes(val)))
		print(prettify_record(record))

COMMANDS = {
	"info": (print_info, "print CAR header and repo info"),
	"list": (list_all, "list all records in the CAR (values as CIDs)"),
	"dump": (dump_all, "dump all records in the CAR (values as JSON)"),
	"dump_record": (dump_record, "dump a single record keyed on ('collection/rkey')"),
}

def print_help():
	print("USAGE: cartool COMMAND [args...]")
	print("")
	print("Available commands:")
	for cmdname, (cmdfn, helptext) in COMMANDS.items():
		args = [f"<{arg}>" for arg in cmdfn.__code__.co_varnames[:cmdfn.__code__.co_argcount]]
		print(f"{cmdname} {' '.join(args)} : {helptext}")

def main():
	if len(sys.argv) < 2:
		print_help()
		return

	command, *args = sys.argv[1:]
	COMMANDS[command][0](*args)

if __name__ == "__main__":
	main()
