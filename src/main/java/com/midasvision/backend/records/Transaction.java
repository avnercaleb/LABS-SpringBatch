package com.midasvision.backend.records;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;

public record Transaction(

        Long id,
        Integer tipo,
        Date data,
        BigDecimal valor,
        Long cpf,
        String cartao,
        Time hora,
        String donoDaLoja,
        String nomeDaLoja) {
}
