(ns me.raynes.fs.compression
  "Compression utilities."
  (:require [clojure.java.io :as io]
            [me.raynes.fs :as fs])
  (:import (java.io FileInputStream InputStreamReader BufferedReader
                    FileOutputStream OutputStreamWriter BufferedOutputStream)
           (java.util.zip ZipFile GZIPInputStream  GZIPOutputStream)
           (org.apache.commons.compress.archivers.tar TarArchiveInputStream
                                                      TarArchiveEntry)
           (org.apache.commons.compress.compressors
                    bzip2.BZip2CompressorInputStream
                    bzip2.BZip2CompressorOutputStream
                       xz.XZCompressorInputStream
                       xz.XZCompressorOutputStream)))

(defn unzip
  "Takes the path to a zipfile source and unzips it to target-dir."
  ([source]
     (unzip source (name source)))
  ([source target-dir]
     (let [zip (ZipFile. (fs/file source))
           entries (enumeration-seq (.entries zip))
           target-file #(fs/file target-dir (str %))]
       (doseq [entry entries :when (not (.isDirectory ^java.util.zip.ZipEntry entry))
               :let [f (target-file entry)]]
         (fs/mkdirs (fs/parent f))
         (io/copy (.getInputStream zip entry) f)))
     target-dir))

(defn- add-zip-entry
  "Add a zip entry. Works for strings and byte-arrays."
  [^java.util.zip.ZipOutputStream zip-output-stream [^String name content & remain]]
  (.putNextEntry zip-output-stream (java.util.zip.ZipEntry. name))
  (if (string? content) ;string and byte-array must have different methods
    (doto (java.io.PrintStream. zip-output-stream true)
      (.print content))
    (.write zip-output-stream ^bytes content))
  (.closeEntry zip-output-stream)
  (when (seq (drop 1 remain))
    (recur zip-output-stream remain)))

(defn make-zip-stream
  "Create zip file(s) stream. You must provide a vector of the
  following form: [[filename1 content1][filename2 content2]...].

  You can provide either strings or byte-arrays as content.

  The piped streams are used to create content on the fly, which means
  this can be used to make compressed files without even writing them
  to disk." [& filename-content-pairs]
  (let [file
        (let [pipe-in (java.io.PipedInputStream.)
              pipe-out (java.io.PipedOutputStream. pipe-in)]
          (future
            (with-open [zip (java.util.zip.ZipOutputStream. pipe-out)]
              (add-zip-entry zip (flatten filename-content-pairs))))
          pipe-in)]
    (io/input-stream file)))

(defn zip
  "Create zip file(s) on the fly. You must provide a vector of the
  following form: [[filename1 content1][filename2 content2]...].

  You can provide either strings or byte-arrays as content."
  [filename & filename-content-pairs]
  (io/copy (make-zip-stream filename-content-pairs)
           (fs/file filename)))

(defn- tar-entries
  "Get a lazy-seq of entries in a tarfile."
  [^TarArchiveInputStream tin]
  (when-let [entry (.getNextTarEntry tin)]
    (cons entry (lazy-seq (tar-entries tin)))))

(defn untar
  "Takes a tarfile source and untars it to target."
  ([source] (untar source (name source)))
  ([source target]
     (with-open [tin (TarArchiveInputStream. (io/input-stream (fs/file source)))]
       (doseq [^TarArchiveEntry entry (tar-entries tin) :when (not (.isDirectory entry))
               :let [output-file (fs/file target (.getName entry))]]
         (fs/mkdirs (fs/parent output-file))
         (io/copy tin output-file)))))

(defn gunzip
  "Takes a path to a gzip file source and unzips it."
  ([source] (gunzip source (name source)))
  ([source target]
     (io/copy (-> source fs/file io/input-stream GZIPInputStream.)
              (fs/file target))))

(defn bunzip2
  "Takes a path to a bzip2 file source and uncompresses it."
  ([source] (bunzip2 source (name source)))
  ([source target]
     (io/copy (-> source fs/file io/input-stream BZip2CompressorInputStream.)
              (fs/file target))))

(defn unxz
  "Takes a path to a xz file source and uncompresses it."
  ([source] (unxz source (name source)))
  ([source target]
    (io/copy (-> source fs/file io/input-stream XZCompressorInputStream.)
             (fs/file target))))

(defn ^:private  ^String encoding [opts]
  (or (:encoding opts) "UTF-8"))

(defn ^:private  ^String compression [opts]
  (or (:encoding opts) "UTF-8"))

(defn- ^Boolean append? [opts]
  (boolean (:append opts)))

(deftype FileGZ [ fname ])

(extend FileGZ
  io/IOFactory
  (assoc io/default-streams-impl
    :make-input-stream
      (fn [x opts]
        (-> (. x fname)
            FileInputStream.
            GZIPInputStream.
            (io/make-input-stream opts)))
     :make-output-stream
       (fn [x opts]
         (-> (. x fname)
             (FileOutputStream. (append? opts))
             GZIPOutputStream.
             (io/make-output-stream opts)))))

(deftype FileBZIP2 [ fname ])

(extend FileBZIP2
  io/IOFactory
  (assoc io/default-streams-impl
    :make-input-stream
      (fn [x opts]
        (-> (. x fname)
            FileInputStream.
            BZip2CompressorInputStream.
            (io/make-input-stream opts)))
     :make-output-stream
       (fn [x opts]
         (-> (. x fname)
             (FileOutputStream. (append? opts))
             BZip2CompressorOutputStream.
             (io/make-output-stream opts)))))

(deftype FileXZ [ fname ])

(extend FileXZ
  io/IOFactory
  (assoc io/default-streams-impl
    :make-input-stream
      (fn [x opts]
        (-> (. x fname)
            FileInputStream.
            XZCompressorInputStream.
            (io/make-input-stream opts)))
     :make-output-stream
       (fn [x opts]
         (-> (. x fname)
             (FileOutputStream. (append? opts))
             XZCompressorOutputStream.
             (io/make-output-stream opts)))))

(defn gz-reader [ fname & opts ]
  (io/make-reader (FileGZ. fname) (when opts (apply hash-map opts))))

(defn bzip2-reader [ fname & opts ]
  (io/make-reader (FileBZIP2. fname) (when opts (apply hash-map opts))))

(defn xz-reader [ fname & opts ]
  (io/make-reader (FileXZ. fname) (when opts (apply hash-map opts))))

(defn gz-writer [ fname & opts ]
  (io/make-writer (FileGZ. fname) (when opts (apply hash-map opts))))

(defn bzip2-writer [ fname & opts ]
  (io/make-writer (FileBZIP2. fname) (when opts (apply hash-map opts))))

(defn xz-writer [ fname & opts]
  (io/make-writer (FileXZ. fname) (when opts (apply hash-map opts))))

(defn slurp*
  [ fname & options])

(defn spit*
  [ fname & options])
