package com.klarna.hiverunner.data;

import java.io.File;
import java.util.List;

public interface FileParser {

  List<Object[]> parse(File file, String... names);

}
