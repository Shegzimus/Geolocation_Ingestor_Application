output "cloud_run_url" {
  description = "Deployed Cloud Run service URL"
  value       = google_cloud_run_service.restaurant_locator.status[0].url
}
