// Copyright 2009 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.enterprise.connector.spi.TraversalContext;

import eu.medsea.mimeutil.MimeType;
import eu.medsea.mimeutil.MimeUtil2;
import eu.medsea.mimeutil.detector.ExtensionMimeDetector;
import eu.medsea.mimeutil.detector.MagicMimeMimeDetector;
import eu.medsea.util.EncodingGuesser;

/**
 * Detector for mime type based on file name and content.
 */
public class MimeTypeFinder {
	private final MimeUtil2 delegate;

	/**
	 * Mime type for documents whose mime type cannot be determined.
	 */
	public static final String UNKNOWN_MIME_TYPE = MimeUtil2.UNKNOWN_MIME_TYPE.toString();

	public MimeTypeFinder() {
		registerEncodingsIfNotSet();
		delegate = new MimeUtil2();
		// register ExtensionMimeDetector
		delegate.registerMimeDetector(ExtensionMimeDetector.class.getName());
		// register MagicMimeMimeDetector
		delegate.registerMimeDetector(MagicMimeMimeDetector.class.getName());
	}

	/**
	 * Sets supported encodings for the mime-util library if they have not been
	 * set. Since the supported encodings is stored as a static Set we
	 * synchronize access.
	 */
	private static void registerEncodingsIfNotSet() {
		if (EncodingGuesser.getSupportedEncodings().size() == 0) {
			Set<String> enc = new HashSet<String>();
			enc.addAll(Arrays.asList("UTF-8", "ISO-8859-1", "windows-1252"));
			enc.add(EncodingGuesser.getDefaultEncoding());
			EncodingGuesser.setSupportedEncodings(enc);
		}
	}

	/**
	 * Returns the mime type for the file with the provided name and content.
	 * 
	 * @throws IOException
	 */

	String find(byte[] file, TraversalContext traversalContext) {
		Collection<MimeType> mimeTypes = getMimeTypes(file);
		String bestMimeType = pickBestMimeType(traversalContext, mimeTypes);
		return bestMimeType;
	}

	@SuppressWarnings("unchecked")
	private Collection<MimeType> getMimeTypes(byte[] file) {
		return delegate.getMimeTypes(file);
	}

	/**
	 * This method detects and returns the most suitable MIME type of the
	 * document.
	 * 
	 * @param traversalContext
	 * @param mimeTypes
	 * @return most suitable MIME type for current document
	 */
	private String pickBestMimeType(TraversalContext traversalContext,
			Collection<MimeType> mimeTypes) {
		if (mimeTypes.size() == 0) {
			return UNKNOWN_MIME_TYPE;
		} else if (mimeTypes.size() == 1) {
			return mimeTypeStringValue(mimeTypes.toArray(new MimeType[1])[0]);
		} else {
			HashSet<String> mimeTypeNames = new HashSet<String>(
					mimeTypes.size());
			for (MimeType mimeType : mimeTypes) {
				mimeTypeNames.add(mimeTypeStringValue(mimeType));
			}
			// get the most suitable MIME type for this document
			return traversalContext.preferredMimeType(mimeTypeNames);
		}
	}

	private String mimeTypeStringValue(MimeType mimeType) {
		return mimeType.getMediaType() + "/" + mimeType.getSubType();
	}
}
