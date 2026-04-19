import jobs_scrape_pipeline, jobs_alerts_pipeline, standardization_pipeline, dubzl_crs, crswth_crs, allsopp_crs
# import jobs_scrape_pipeline, jobs_alerts_pipeline, standardization_pipeline, dubzl_crs, crswth_crs
# from plombery import get_app  # noqa: F401



if __name__ == "__main__":
    import uvicorn
    import errno

    try:
        uvicorn.run("plombery:get_app", reload=True, factory=True, reload_dirs="..", port=8080, host="0.0.0.0")
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            print(f"ERROR: Port 8080 is already in use. Kill the existing process or change the port.")
            print("  Try: lsof -i :8080  or  fuser -k 8080/tcp")
        else:
            raise
