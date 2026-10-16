from etl_project.pipeline import run_pipeline

if __name__ == "__main__":
    result = run_pipeline()

    print("\n========== PIPELINE SUMMARY ==========")

    for key, value in result.items():
        print(f"{key}: {value}")