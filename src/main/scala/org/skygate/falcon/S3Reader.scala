package org.skygate.falcon

import java.io.InputStream
import java.nio.charset.CodingErrorAction
import java.security.{KeyFactory, KeyPair, PrivateKey, PublicKey}
import java.security.spec.{KeySpec, PKCS8EncodedKeySpec, X509EncodedKeySpec}
import java.util.zip.GZIPInputStream
import javax.crypto.spec.SecretKeySpec
import javax.crypto.SecretKey

import scala.io.{Source, Codec}
import scala.util.Try

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3EncryptionClientBuilder}
import com.amazonaws.services.s3.model.{CryptoConfiguration, CryptoMode, EncryptionMaterials, StaticEncryptionMaterialsProvider}
import com.amazonaws.util.Base64

object S3Reader {

  def buildPublicKey(key: String): PublicKey = {
    val data = Base64.decode(key)
    val spec: KeySpec = new X509EncodedKeySpec(data)
    val fact: KeyFactory = KeyFactory.getInstance("RSA")
    fact.generatePublic(spec)
  }

  def buildPrivateKey(key: String): PrivateKey = {
    val data = Base64.decode(key)
    val spec: KeySpec = new PKCS8EncodedKeySpec(data)
    val fact: KeyFactory = KeyFactory.getInstance("RSA")
    fact.generatePrivate(spec)
  }

  def buildAesMaterials(symmetricKey: String,
    materialSet: String, materialSerial: String = "1") : EncryptionMaterials = {

    val data = Base64.decode(symmetricKey)
    val secretKey: SecretKey = new SecretKeySpec(data, 0, data.length, "AES")
    val encryptionMaterials = new EncryptionMaterials(secretKey)
    encryptionMaterials.addDescription("mat-serial", materialSerial)
    encryptionMaterials.addDescription("mat-set", materialSet)
    encryptionMaterials.addDescription("mat-type", "SymmetricKey")

    encryptionMaterials
  }

  def buildRsaMaterials(publicKey: String, privateKey: String,
    materialSet: String, materialSerial: String = "1") : EncryptionMaterials = {

    val encryptionKeyPair = new KeyPair(buildPublicKey(publicKey), buildPrivateKey(privateKey))
    val encryptionMaterials = new EncryptionMaterials(encryptionKeyPair)
    encryptionMaterials.addDescription("mat-serial", materialSerial)
    encryptionMaterials.addDescription("mat-set", materialSet)
    encryptionMaterials.addDescription("mat-type", "PublicKey")

    encryptionMaterials
  }

  def buildS3Client(principal: String, credential: String,
    encryptionMaterials: EncryptionMaterials, region: String): AmazonS3 = {

    val clientConfig = new ClientConfiguration
    clientConfig.setMaxConnections(10)
    clientConfig.setMaxErrorRetry(5)

    val cryptoConfig = new CryptoConfiguration(CryptoMode.AuthenticatedEncryption)
    val awsCredentials = new BasicAWSCredentials(principal, credential)

    AmazonS3EncryptionClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
        .withEncryptionMaterials(new StaticEncryptionMaterialsProvider(encryptionMaterials))
        .withClientConfiguration(clientConfig)
        .withCryptoConfiguration(cryptoConfig)
        .withRegion(region)
        .build()
  }
}

class S3Reader(principal: String, credential: String,
  encryptionMaterials: EncryptionMaterials, region: String = "us-east-1") {

  implicit val decoder = Codec.UTF8
  decoder.onMalformedInput(CodingErrorAction.IGNORE)
  private val s3Client = S3Reader.buildS3Client(principal, credential, encryptionMaterials, region)

  def readObject(bucket: String, objKey: String) : Iterator[String] = {
    val obj = s3Client.getObject(bucket, objKey)
    Try(Source.fromInputStream(obj.getObjectContent: InputStream).getLines).getOrElse(Iterator.empty)
  }

  def readGzippedObject(bucket: String, objKey: String) : Iterator[String] = {
    val obj = s3Client.getObject(bucket, objKey)
    val stream: InputStream = new GZIPInputStream(obj.getObjectContent)
    Try(Source.fromInputStream(stream).getLines).getOrElse(Iterator.empty)
  }

  def readObjects(bucket: String, manifest: Iterator[String]) : Seq[String] = {
    val prefix = s"s3://${bucket}/"
    val keys = manifest.filter(line => line.endsWith(".gz")).map(s => s.substring(prefix.length, s.length)).toSeq

    val lines = keys.flatMap(key => {
      readGzippedObject(bucket, key)
    })

    lines
  }

  override def toString = s"${principal}, ${region}, ${encryptionMaterials.getMaterialsDescription()}"
}