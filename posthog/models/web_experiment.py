import json
from dataclasses import asdict, dataclass
from typing import Optional, Any

from django.db import models
from posthog.models import Experiment

class WebExperimentManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(type="web")

class WebExperiment(Experiment):
    objects = WebExperimentManager()

    class Meta:
        proxy = True
