package com.midasvision.backend.errors;

import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class ExceptionHandlerController {

    @ExceptionHandler(JobInstanceAlreadyCompleteException.class)
    private ResponseEntity<Object> handleFileAlreadyImported(
            JobInstanceAlreadyCompleteException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body("O arquivo informado ja foi importado no sistema!");
    }
}
