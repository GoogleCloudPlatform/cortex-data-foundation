# Google Cloud Cortex Framework for Meridian

<walkthrough-tutorial-duration duration="30min"></walkthrough-tutorial-duration>

## What is Google Cloud Cortex Framework for Meridian?

One of the key value propositions of Google Cloud Cortex Framework is to provide a Data & AI foundation for next generation enterprise intelligence that enables analytics spanning across key areas such as sales and marketing, order fulfillment and inventory management.

Cortex for Marketing provides cross media platform KPIs and metrics. These metrics are a significant part of the pre-modeling data preparation step for running Google’s latest Marketing Mix Model (MMM) called Meridian. Advertisers can accelerate the pre-modeling data preparation process by leveraging Cortex Data Foundation.

For more information, see:

- [Google Cloud Cortex Framework](https://cloud.google.com/cortex/docs/overview)
- [Cortex Framework for Marketing](https://cloud.google.com/cortex/docs/data-sources-and-workloads#marketing)
- [Meridian](https://developers.google.com/meridian)

This tutorial will guide you through demo deployment of Cortex Framework for Meridian in your own Google Cloud project.

This deployment includes:

- Cortex Marketing with Cross Media with sample marketing data for Google Ads, YouTube, Meta and TikTik
- Cortex for Meridian
- Oracle EBS with sample sales data
- Automated Cortex for Meridian notebook execution with sample reports saved to GCS

For a full production deployment please follow the [full deployment steps](https://cloud.google.com/cortex/docs/overview#deployment)

Click the **Start** button to move to the next step.

## Please select a project for deployment or create one.

<walkthrough-project-setup billing=true></walkthrough-project-setup>

## Confirm project and region

Cortex for Meridian will be installed in the selected project: `<walkthrough-project-id/>`

The default region used is: `us-central1`. Make sure you select this when viewing e.g. Colab Enterprise runtime templates and executions.

Click the **Next** button to move to the next step.

## Start deployment

<walkthrough-cloud-shell-icon></walkthrough-cloud-shell-icon>

❗️IMPORTANT❗️The default configuration parameters and sample data for Meridian are intended for demo purposes only and should not be deployed for production use. Meridian configuration parameters should be chosen with great care as they will influence the behaviour of the model and results. Please consult Meridian documentation for guidance on how to setup the model configuration for your unique business needs and goals. See [Meridian modeling](https://developers.google.com/meridian/docs/basics/about-the-project). If needed consult with an official [Google Meridian partner](https://developers.google.com/meridian/partners) and/or your Google Ads representative.

```sh
./1_click_meridian.sh --project "<walkthrough-project-id/>"
```

<walkthrough-footnote>The 1-Click deployment will create Marketing and Oracle EBS sales datasets, GCS buckets, deploy services needed to run Meridian with the sample data and start a Colab Notebook execution. </walkthrough-footnote>

When the script starts click the **Next** button.

## Deployment started

<walkthrough-notification-menu-icon></walkthrough-notification-menu-icon>

The deployment is now running please wait for the Cloud Build deployment to complete. This will take about 25-30 mins.

If the Cloud Shell session times out while the Cloud Build runs the Notebook execution might not be started automatically. In this case you can start it manually in the Google Cloid portal or run this command:

```sh
gcloud workflows execute cortex-meridian-execute-notebook --data="{}" --location=us-central1 --project="<walkthrough-project-id/>"
```

[Go to Cloud Build](https://console.cloud.google.com/cloud-build/builds).

[Go to Workflows](https://console.cloud.google.com/workflows/workflow/us-central1/cortex-meridian-execute-notebook/executions).

[Go to Notebook Executions](https://console.cloud.google.com/vertex-ai/colab/execution-jobs) (select `us-central1` as region)

Click the **Next** button to finish.

## Conclusion

Thank you for trying Google Cloud Cortex Framework for Meridian!

<walkthrough-conclusion-trophy></walkthrough-conclusion-trophy>
