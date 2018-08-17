import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3BucketDemo {

	private static String ACCESS_KEY = "";
	private static String SECRET_KEY = "";
	private static String BUCKET_NAME = "";
	private static String REGION = "";
	private static String FILE_NAME = "";
	private static String FILE_PATH = "" + FILE_NAME;
	private static String DOWNLOAD_NAME = "";
	private static String DOWNLOAD_PATH = "" + DOWNLOAD_NAME;
	private static String PROXY_HOST = "";
	private static Integer PROXY_PORT = 0;

	private AmazonS3 s3Client = null;

	public S3BucketDemo() throws Exception {
		AWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);

		ClientConfiguration clientCfg = new ClientConfiguration();
		clientCfg.setProtocol(Protocol.HTTP);
		clientCfg.setProxyHost(PROXY_HOST);
		clientCfg.setProxyPort(PROXY_PORT);
		s3Client = AmazonS3Client.builder().withRegion(REGION)
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).withClientConfiguration(clientCfg)
				.build();
	}

	public void uploadFile(String bucketName, String fileName, String filePath) {
		System.out.println("Uploading file...");
		try {
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(new File(filePath).length());
			PutObjectRequest request = new PutObjectRequest(bucketName, fileName, new FileInputStream(filePath),
					metadata);
			s3Client.putObject(request);
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void listAllBuckets() {
		System.out.println("Listing all buckets...");
		for (com.amazonaws.services.s3.model.Bucket bucket : s3Client.listBuckets()) {
			System.out.println(bucket.getName());
		}
	}

	public void listAllFiles(String bucketName) {
		System.out.println("Listing all files...");
		ObjectListing listing = s3Client.listObjects(bucketName);
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();

		for (S3ObjectSummary summary : summaries) {
			System.out.println(summary.getKey());
		}
	}

	public void downloadFile(String bucketName, String fileName, String downloadPath) {
		System.out.println("Downloading file...");
		S3Object object = s3Client.getObject(bucketName, fileName);
		try (InputStream in = object.getObjectContent(); OutputStream out = new FileOutputStream(downloadPath)) {
			byte[] buf = new byte[1024];
			int count = 0;
			while ((count = in.read(buf)) != -1) {
				out.write(buf, 0, count);
			}
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void deleteFile(String bucketName, String fileName) {
		System.out.println("Deleting file...");
		s3Client.deleteObject(bucketName, fileName);
	}

	public static void main(String[] args) throws Exception {
		S3BucketDemo s3 = new S3BucketDemo();
		s3.listAllBuckets();
		s3.uploadFile(BUCKET_NAME, FILE_NAME, FILE_PATH);
		s3.listAllFiles(BUCKET_NAME);
		s3.downloadFile(BUCKET_NAME, FILE_NAME, DOWNLOAD_PATH);
		s3.deleteFile(BUCKET_NAME, FILE_NAME);
	}
}
