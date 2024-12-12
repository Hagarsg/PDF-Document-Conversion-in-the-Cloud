package com.example;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import software.amazon.awssdk.services.sqs.model.Message;

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import javax.imageio.ImageIO;

public class Worker {

    final static AWS aws = AWS.getInstance();
    private static String workersQueueUrl = aws.getQueueUrl(aws.getWorkerQueueName());
    private static String responsesQueueUrl = aws.getQueueUrl(aws.getResponsesQueueName());

    public static void main(String[] args) {
        while (true) {
            try {
                // Measure the time taken to receive messages from the queue
                List<Message> messages = aws.receiveMessages(workersQueueUrl);

                if (messages != null && !messages.isEmpty()) {
                    for (Message m : messages) {
                        String[] parts = m.body().split("\t");
                        if (parts.length < 3) {
                            aws.deleteMessage(workersQueueUrl, m);
                            continue;
                        }
                        String operation = parts[0];
                        String pdfUrl = parts[1];
                        String fileId = parts[2];

                        File pdfFile = null;
                        File resultFile = null;

                        try {
                            // Step 1: Download PDF
                            pdfFile = downloadPDF(pdfUrl);

                            if (pdfFile == null || !pdfFile.exists() || pdfFile.length() == 0) {
                                throw new IOException("Invalid or empty PDF file.");
                            }

                            // Step 2: Perform Operation
                            resultFile = performOperation(operation, pdfFile);

                            // Step 3: Upload Result
                            String s3ResultsPath = aws.getResultsS3Name() + m.messageId();
                            String resultS3Url = aws.uploadFileToS3(s3ResultsPath, resultFile);

                            // Measure the time taken to send the response message
                            String responseMessage = String.format("%s\t%s\t%s\t%s", operation, pdfUrl, s3ResultsPath, fileId);
                            aws.sendMessage(responsesQueueUrl, responseMessage);

                        } catch (IOException e) {
                            String errorMsg = "Error processing task: " + e.getMessage();
                            String responseMessage = String.format("%s\t%s\t%s\t%s", operation, pdfUrl, errorMsg, fileId);
                            aws.sendMessage(responsesQueueUrl, responseMessage);
                        } finally {
                            // Measure the time taken to delete the message
                            aws.deleteMessage(workersQueueUrl, m);

                            deleteFile(pdfFile);
                            deleteFile(resultFile);
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error polling SQS or processing messages: " + e.getMessage());
            }
        }
    }

    private static File downloadPDF(String pdfUrl) throws IOException {
        File pdfFile = new File("downloaded.pdf");
        try (InputStream inputStream = new URL(pdfUrl).openStream();
             FileOutputStream fileOutputStream = new FileOutputStream(pdfFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
        }
        return pdfFile;
    }

    private static File performOperation(String operation, File pdfFile) throws IOException {
        File resultFile;
        switch (operation) {
            case "ToImage":
                resultFile = convertToImage(pdfFile);
                break;
            case "ToHTML":
                resultFile = convertToHTML(pdfFile);
                break;
            case "ToText":
                resultFile = convertToText(pdfFile);
                break;
            default:
                throw new IllegalArgumentException("Unknown operation: " + operation);
        }
        return resultFile;
    }

    private static File convertToImage(File pdfFile) throws IOException {
        PDDocument document = PDDocument.load(pdfFile);
        PDFRenderer renderer = new PDFRenderer(document);
        BufferedImage image = renderer.renderImage(0);
        File outputFile = new File("output.png");
        ImageIO.write(image, "png", outputFile);
        document.close();
        return outputFile;
    }

    private static File convertToHTML(File pdfFile) throws IOException {
        PDDocument document = PDDocument.load(pdfFile);
        PDFTextStripper stripper = new PDFTextStripper();
        String text = stripper.getText(document);
        File outputFile = new File("output.html");
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write("<html><body><pre>" + text + "</pre></body></html>");
        }
        document.close();
        return outputFile;
    }

    private static File convertToText(File pdfFile) throws IOException {
        PDDocument document = PDDocument.load(pdfFile);
        PDFTextStripper stripper = new PDFTextStripper();
        String text = stripper.getText(document);
        File outputFile = new File("output.txt");
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write(text);
        }
        document.close();
        return outputFile;
    }

    private static void deleteFile(File file) {
        if (file != null && file.exists()) {
            try {
                Files.delete(file.toPath());
            } catch (IOException e) {
                System.err.println("Failed to delete file: " + file.getAbsolutePath());
            }
        }
    }
}