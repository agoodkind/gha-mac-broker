#include "aria2_shim.h"

#include <string>
#include <vector>

#include <aria2/aria2.h>

extern "C" int a2_download(const char *const *uris, const char *const *dirs,
                           const char *const *outs, int n, const char *header,
                           int split, int max_conn_per_server,
                           int max_concurrent) {
  using namespace aria2;
  if (n <= 0) {
    return 0;
  }
  if (libraryInit() != 0) {
    return -1;
  }
  SessionConfig config;
  config.keepRunning = false;
  config.useSignalHandler = false;
  KeyVals global;
  global.push_back({"max-concurrent-downloads", std::to_string(max_concurrent)});
  Session *session = sessionNew(global, config);
  if (session == nullptr) {
    libraryDeinit();
    return -2;
  }
  std::vector<A2Gid> gids;
  int addFailed = 0;
  for (int i = 0; i < n; ++i) {
    std::vector<std::string> uri{std::string(uris[i])};
    KeyVals opts;
    opts.push_back({"dir", std::string(dirs[i])});
    opts.push_back({"out", std::string(outs[i])});
    opts.push_back({"split", std::to_string(split)});
    opts.push_back(
        {"max-connection-per-server", std::to_string(max_conn_per_server)});
    opts.push_back({"min-split-size", "1M"});
    opts.push_back({"continue", "true"});
    opts.push_back({"allow-overwrite", "true"});
    opts.push_back({"auto-file-renaming", "false"});
    if (header != nullptr && header[0] != '\0') {
      opts.push_back({"header", std::string(header)});
    }
    A2Gid gid = 0;
    if (addUri(session, &gid, uri, opts) == 0) {
      gids.push_back(gid);
    } else {
      ++addFailed;
    }
  }
  int rv;
  do {
    rv = run(session, RUN_DEFAULT);
  } while (rv == 1);
  int failed = addFailed;
  for (A2Gid gid : gids) {
    DownloadHandle *dh = getDownloadHandle(session, gid);
    if (dh == nullptr) {
      ++failed;
      continue;
    }
    if (dh->getStatus() != DOWNLOAD_COMPLETE || dh->getErrorCode() != 0) {
      ++failed;
    }
    deleteDownloadHandle(dh);
  }
  sessionFinal(session);
  libraryDeinit();
  if (rv < 0 && failed == 0) {
    return -3;
  }
  return failed;
}
