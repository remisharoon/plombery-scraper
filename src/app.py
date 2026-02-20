import jobs_scrape_pipeline, jobs_alerts_pipeline, standardization_pipeline, dubzl_crs, crswth_crs, allsopp_crs
# import jobs_scrape_pipeline, jobs_alerts_pipeline, standardization_pipeline, dubzl_crs, crswth_crs
# from plombery import get_app  # noqa: F401



if __name__ == "__main__":
    import uvicorn

    uvicorn.run("plombery:get_app", reload=True, factory=True, reload_dirs="..", port=8080, host="0.0.0.0")
