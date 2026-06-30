#ifndef ARIA2_SHIM_H
#define ARIA2_SHIM_H

#ifdef __cplusplus
extern "C" {
#endif

/* a2_download fetches n URIs in parallel via libaria2. Download i writes to
   dirs[i]/outs[i]. header, when non-empty, is added as an HTTP request header on
   every download (for example "Authorization: Bearer <token>"). split and
   max_conn_per_server bound the per-download connection count; max_concurrent
   bounds simultaneous downloads. Returns the number of failed downloads (0 on
   full success) or a negative value on a library-level error. */
int a2_download(const char *const *uris, const char *const *dirs,
                const char *const *outs, int n, const char *header, int split,
                int max_conn_per_server, int max_concurrent);

#ifdef __cplusplus
}
#endif

#endif /* ARIA2_SHIM_H */
