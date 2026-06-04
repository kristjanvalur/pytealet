import _tealet
import _tealet_capi_client


def test_capi_client_api_info():
    info = _tealet_capi_client.api_info()

    assert info["abi_version"] == _tealet.C_API_ABI_VERSION
    assert info["struct_size"] >= 0
    assert info["feature_flags"] >= 0
    assert info["has_run"] is True
    assert info["has_switch"] is True


def test_capi_client_current_is_main():
    assert _tealet_capi_client.current_is_main() is True


def test_capi_client_check_tealet():
    assert _tealet_capi_client.check_tealet(_tealet.current()) is True
    assert _tealet_capi_client.check_tealet(object()) is False


def test_capi_client_switch_roundtrip():
    def parked(current, _arg):
        resumed = current.main().switch("paused")
        return current.main(), resumed

    t = _tealet.tealet()
    assert t.run(parked, None) == "paused"
    assert _tealet_capi_client.capi_switch(t, "resumed") == "resumed"


def test_capi_client_run_forwarding():
    def worker(current, arg):
        return current.main(), ("via-capi-run", arg)

    t = _tealet.tealet()
    assert _tealet_capi_client.capi_run(t, worker, 123) == ("via-capi-run", 123)
