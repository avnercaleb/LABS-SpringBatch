package com.midasvision.backend.controller;

import com.midasvision.backend.service.CNABService;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("cnab")
public class CNABController {

    private final CNABService service;

    public CNABController(CNABService service) {
        this.service = service;
    }

    @PostMapping("upload")
    public String upload(@RequestParam("file")MultipartFile file) throws Exception{
        service.uploadCnabFile(file);
        return "Processamento iniciado!";
    }
}
