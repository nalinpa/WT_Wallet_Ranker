# Cloud Run Deployment Script for Wallet Scoring Service (PowerShell)

param(
    [Parameter(Mandatory=$false)]
    [string]$ProjectId = "wtwalletranker",
    
    [Parameter(Mandatory=$false)]
    [string]$Region = "asia-southeast1",
    
    [Parameter(Mandatory=$false)]
    [string]$ServiceName = "wallet-scoring-service",
    
    [Parameter(Mandatory=$false)]
    [string]$DatasetId = "crypto_analysis"
)

# Function to check if command exists
function Test-Command {
    param($Command)
    try {
        Get-Command $Command -ErrorAction Stop
        return $true
    } catch {
        return $false
    }
}

Write-Host "🚀 Deploying Wallet Scoring Service to Google Cloud Run" -ForegroundColor Green
Write-Host "Project: $ProjectId" -ForegroundColor Cyan
Write-Host "Region: $Region" -ForegroundColor Cyan
Write-Host "Service: $ServiceName" -ForegroundColor Cyan

# Check if gcloud is installed
if (-not (Test-Command "gcloud")) {
    Write-Host "❌ Google Cloud CLI (gcloud) not found!" -ForegroundColor Red
    Write-Host "Please install it from: https://cloud.google.com/sdk/docs/install" -ForegroundColor Yellow
    exit 1
}

# Check if bq is installed
if (-not (Test-Command "bq")) {
    Write-Host "❌ BigQuery CLI (bq) not found!" -ForegroundColor Red
    Write-Host "Please install it as part of Google Cloud CLI" -ForegroundColor Yellow
    exit 1
}

try {
    # Set the project
    Write-Host "📋 Setting up project..." -ForegroundColor Yellow
    gcloud config set project $ProjectId
    if ($LASTEXITCODE -ne 0) { throw "Failed to set project" }

    # Enable required APIs
    Write-Host "🔌 Enabling required APIs..." -ForegroundColor Yellow
    $apis = @(
        "run.googleapis.com",
        "bigquery.googleapis.com", 
        "cloudbuild.googleapis.com"
    )
    
    foreach ($api in $apis) {
        Write-Host "  Enabling $api..." -ForegroundColor Gray
        gcloud services enable $api
        if ($LASTEXITCODE -ne 0) { throw "Failed to enable $api" }
    }

    # Create BigQuery dataset if it doesn't exist
    Write-Host "💾 Creating BigQuery dataset..." -ForegroundColor Yellow
    $datasetRef = "${ProjectId}:${DatasetId}"
    bq mk --dataset --location=US $datasetRef 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  Created dataset $datasetRef" -ForegroundColor Green
    } else {
        Write-Host "  Dataset $datasetRef already exists or creation failed" -ForegroundColor Gray
    }

    # Deploy to Cloud Run
    Write-Host "🚢 Deploying to Cloud Run..." -ForegroundColor Yellow
    
    $deployArgs = @(
        "run", "deploy", $ServiceName,
        "--source", ".",
        "--platform", "managed",
        "--region", $Region,
        "--allow-unauthenticated",
        "--set-env-vars", "PROJECT_ID=$ProjectId",
        "--set-env-vars", "DATASET_ID=$DatasetId",
        "--set-env-vars", "ENVIRONMENT=production",
        "--memory", "2Gi",
        "--cpu", "2",
        "--min-instances", "0",
        "--max-instances", "10",
        "--timeout", "900",
        "--concurrency", "80"
    )
    
    & gcloud @deployArgs
    if ($LASTEXITCODE -ne 0) { throw "Failed to deploy to Cloud Run" }

    # Get the service URL
    Write-Host "🔍 Getting service URL..." -ForegroundColor Yellow
    $serviceUrl = gcloud run services describe $ServiceName --region $Region --format "value(status.url)"
    
    Write-Host ""
    Write-Host "✅ Deployment complete!" -ForegroundColor Green
    Write-Host "Service URL: $serviceUrl" -ForegroundColor Cyan
    Write-Host ""
    
    Write-Host "🧪 Test your deployment:" -ForegroundColor Yellow
    Write-Host "Invoke-RestMethod -Uri '$serviceUrl/health'" -ForegroundColor Gray
    Write-Host ""
    
    Write-Host "📊 View logs:" -ForegroundColor Yellow
    Write-Host "gcloud run services logs read $ServiceName --region $Region" -ForegroundColor Gray
    Write-Host ""
    
    Write-Host "🔧 Update service:" -ForegroundColor Yellow
    Write-Host "gcloud run deploy $ServiceName --source . --region $Region" -ForegroundColor Gray
    Write-Host ""
    
    Write-Host "📚 View service in console:" -ForegroundColor Yellow
    Write-Host "https://console.cloud.google.com/run/detail/$Region/$ServiceName/metrics?project=$ProjectId" -ForegroundColor Gray

} catch {
    Write-Host ""
    Write-Host "❌ Deployment failed: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "💡 Troubleshooting tips:" -ForegroundColor Yellow
    Write-Host "1. Make sure you're authenticated: gcloud auth login" -ForegroundColor Gray
    Write-Host "2. Check your project ID is correct: gcloud projects list" -ForegroundColor Gray
    Write-Host "3. Ensure billing is enabled for your project" -ForegroundColor Gray
    Write-Host "4. Check the error messages above for specific issues" -ForegroundColor Gray
    exit 1
}