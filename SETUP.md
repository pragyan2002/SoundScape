# AWS Setup Guide — SoundScape

> **Cost guarantee**: Steps below use only S3 free tier (5 GB storage, 20K GET,
> 2K PUT per month). No Glue, Athena, EMR, or Lambda. All compute is local.
> Flagged cost risks are marked **[COST RISK]**.

---

## Prerequisites

```bash
aws --version       # >= 2.x
aws configure list  # verify default profile is set
```

---

## 1. Create S3 Bucket

Pick a **globally unique** name. Replace `YOUR_BUCKET_NAME` everywhere below.

```bash
# Create bucket (us-east-1 is free-tier eligible)
aws s3api create-bucket \
  --bucket YOUR_BUCKET_NAME \
  --region us-east-1

# Block all public access (security hardening)
aws s3api put-public-access-block \
  --bucket YOUR_BUCKET_NAME \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

> **[COST RISK]** S3 free tier is 5 GB storage + 20,000 GET + 2,000 PUT per
> month for 12 months (new accounts). The enriched Parquet dataset will be
> ~50–100 MB total. Well within limits. **Watch out**: if you accidentally
> enable S3 versioning, deleted objects still accrue storage costs.
> DO NOT enable versioning.

---

## 2. Create Folder Prefixes

S3 has no real folders — these just seed the prefix structure so paths exist
before the pipeline writes to them.

```bash
# Raw ingestion output
aws s3api put-object \
  --bucket YOUR_BUCKET_NAME \
  --key raw/tracks/

# Enriched pipeline output  
aws s3api put-object \
  --bucket YOUR_BUCKET_NAME \
  --key enriched/tracks/
```

---

## 3. Create Scoped IAM User

Creates `soundscape-local` with **least-privilege** access: only the
SoundScape bucket, no other AWS resources.

### 3a. Create the user

```bash
aws iam create-user --user-name soundscape-local
```

### 3b. Create the inline policy

Save this as `soundscape-policy.json` (do not commit this file):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SoundScapeBucketAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR_BUCKET_NAME",
        "arn:aws:s3:::YOUR_BUCKET_NAME/*"
      ]
    }
  ]
}
```

### 3c. Attach the policy

```bash
aws iam put-user-policy \
  --user-name soundscape-local \
  --policy-name SoundScapeBucketPolicy \
  --policy-document file://soundscape-policy.json
```

### 3d. Create access keys

```bash
aws iam create-access-key --user-name soundscape-local
```

This prints `AccessKeyId` and `SecretAccessKey`. Configure a named profile:

```bash
aws configure --profile soundscape-local
# AWS Access Key ID:     <AccessKeyId from above>
# AWS Secret Access Key: <SecretAccessKey from above>
# Default region name:   us-east-1
# Default output format: json
```

---

## 4. Configure .env

```bash
cp .env.example .env
```

Edit `.env`:

```
AWS_PROFILE=soundscape-local
S3_BUCKET_NAME=YOUR_BUCKET_NAME
GEMINI_API_KEY=your_gemini_api_key_here
```

---

## 5. Verify Setup

```bash
# Test bucket access with the scoped profile
aws s3 ls s3://YOUR_BUCKET_NAME/ --profile soundscape-local

# Expected output:
#   PRE enriched/
#   PRE raw/
```

---

## Cost Risks Summary

| Service | Usage | Risk | Notes |
|---------|-------|------|-------|
| S3 storage | ~100 MB Parquet | None (free tier: 5 GB) | Monitor in AWS Console |
| S3 PUT requests | ~200 (ingest + enrich) | None (free tier: 2K/mo) | |
| S3 GET requests | ~500 (enrich reads + DuckDB) | None (free tier: 20K/mo) | |
| S3 data transfer | ~100 MB out to local | **[COST RISK]** | $0.09/GB after 1 GB/mo free. Pipeline moves ~100 MB total — expect <$0.01 if any charge |
| IAM | User + policy | None | Free |
| Glue / Athena / EMR | **NOT USED** | None | Explicitly excluded from this project |

> **Bottom line**: expected spend = $0. The only realistic charge is ~$0.01
> in S3 egress if you iterate many times. Check AWS Cost Explorer after first run.

---

## Gemini API Setup

1. Go to [Google AI Studio](https://aistudio.google.com/)
2. Create API key → copy into `.env` as `GEMINI_API_KEY`
3. Free tier: 15 RPM, 1M TPD on `gemini-1.5-flash` — sufficient for 13K tracks
   in batches of 10

---

## Cleanup (when done)

```bash
# Delete all objects then the bucket
aws s3 rm s3://YOUR_BUCKET_NAME --recursive --profile soundscape-local
aws s3api delete-bucket --bucket YOUR_BUCKET_NAME --region us-east-1

# Delete IAM user
aws iam delete-user-policy \
  --user-name soundscape-local \
  --policy-name SoundScapeBucketPolicy
aws iam delete-access-key \
  --user-name soundscape-local \
  --access-key-id YOUR_ACCESS_KEY_ID
aws iam delete-user --user-name soundscape-local
```
